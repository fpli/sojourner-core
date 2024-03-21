package com.ebay.sojourner.cjs.service;

import com.ebay.sojourner.cjs.model.SignalDefinition;
import com.ebay.sojourner.cjs.util.CjsLkpRefreshTimeTask;
import com.ebay.sojourner.cjs.util.JsonUtils;
import com.ebay.sojourner.cjs.util.SignalContext;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import lombok.AccessLevel;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.ebay.sojourner.cjs.util.CJSConfig.getCJSProperty;
import static com.ebay.sojourner.cjs.util.CJSConfig.getString;
import static com.ebay.sojourner.common.util.Property.CJSBETA_METADATA_JSON;
import static com.ebay.sojourner.common.util.Property.CJS_LKP_PATH;
import static com.ebay.sojourner.common.util.Property.CJS_METADATA_JSON;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;

@Slf4j
public class CjsMetadataManager implements Serializable, Closeable {

    public static final TypeReference<List<SignalDefinition>> SIGNAL_TYPE_REF =
            new TypeReference<List<SignalDefinition>>() {
            };

    private static volatile CjsMetadataManager cjsMetadataManager;

    private final CjsLkpRefreshTimeTask lkpRefreshTimeTask;

    // region data fields
    @Setter(AccessLevel.PRIVATE)
    private transient volatile Map<String, SignalDefinition> cjsMetadata;
    @Setter(AccessLevel.PRIVATE)
    private transient volatile Map<String, SignalDefinition> cjsBetaMetadata;
    // endregion

    // region control fields
    private transient volatile boolean initializedFlg = false;
    private transient volatile FileSystem fileSystem = null;
    private final String lkpPath = getCJSProperty(CJS_LKP_PATH);
    private final transient Map<String, Long> lkpFileLastUpdDt = new ConcurrentHashMap<>();
    private final transient Map<String, Long> lkpFileLastFileSystemUpdDt = new ConcurrentHashMap<>();
    // endregion

    // region mock data for validation
    private static final SignalContext validationData = genCjsSyntaxValidationContext();
    // endregion

    private CjsMetadataManager(TimeUnit timeUnit) {
        initialize();
        lkpRefreshTimeTask = new CjsLkpRefreshTimeTask(this, timeUnit);
    }

    private CjsMetadataManager() {
        this(TimeUnit.HOURS);
    }

    public static CjsMetadataManager getInstance() {
        if (cjsMetadataManager == null) {
            synchronized (CjsMetadataManager.class) {
                if (cjsMetadataManager == null) {
                    cjsMetadataManager = new CjsMetadataManager();
                }
            }
        }
        return cjsMetadataManager;
    }

    public static Supplier<Map<String, SignalDefinition>> getCjsMetadataProvider() {
        return () -> getInstance().getCjsMetadata();
    }

    public static Supplier<Map<String, SignalDefinition>> getCjsBetaMetadataProvider() {
        return () -> getInstance().getCjsBetaMetadata();
    }

    // region public data methods
    public Map<String, SignalDefinition> getCjsMetadata() {
        if (Objects.isNull(cjsMetadata)) {
            cjsMetadata = new ConcurrentHashMap<>();
        }
        return cjsMetadata;
    }

    public Map<String, SignalDefinition> getCjsBetaMetadata() {
        if (Objects.isNull(cjsBetaMetadata)) {
            cjsBetaMetadata = new ConcurrentHashMap<>();
        }
        return cjsBetaMetadata;
    }
    // endregion

    // region data refresh methods
    public void initialize() {
        refresh();
        initializedFlg = true;
    }

    public synchronized void refresh() {
        refreshCjsMetadata();
        refreshCjsBetaMetadata();
        printLkpFileStatus();
    }

    private void refreshCjsBetaMetadata() {
        refreshCjsMetadataCommon(CJSBETA_METADATA_JSON,
                                 this::setCjsBetaMetadata,
                                 CjsMetadataManager::cjsSyntaxValidator);
    }

    private void refreshCjsMetadata() {
        refreshCjsMetadataCommon(CJS_METADATA_JSON,
                                 this::setCjsMetadata,
                                 CjsMetadataManager::cjsSyntaxValidator);
    }

    private void refreshCjsMetadataCommon(String lkpType,
                                          Consumer<Map<String, SignalDefinition>> updateAction,
                                          Predicate<Map<String, SignalDefinition>> validator) {
        if (isUpdate(lkpType)) {
            try (val cjsStream = getInputStream(lkpType)) {

                val ret = loadAndValidateNewCjsDefinition(updateAction, validator, cjsStream);

                if (ret) {
                    updateLkpFileLastUpdDt(lkpType);
                    return;
                } else {
                    log.error("CJS Metadata validation failed for {}. Retry from CLASSPATH.", lkpType);
                }
            } catch (Exception e) {
                log.error("Error reading json from file {}. Retry from CLASSPATH.", lkpType, e);
            } finally {
                closeFS();
            }

            if (!initializedFlg || lkpFileLastUpdDt.get(getString(lkpType)) == null) {
                try (val cjsClasspathStream = getStreamFromClasspath(getString(lkpType))) {
                    val ret = loadAndValidateNewCjsDefinition(updateAction, validator, cjsClasspathStream);
                    if (!ret) {
                        log.error("CJS Metadata validation failed for {}.", lkpType);
                    }
                } catch (Exception e) {
                    log.error("Fatal error reading json from CLASSPATH {}.", lkpType, e);
                }
            }
        }
    }

    private boolean loadAndValidateNewCjsDefinition(Consumer<Map<String, SignalDefinition>> updateAction,
                                                    Predicate<Map<String, SignalDefinition>> validator,
                                                    InputStream inputStream) throws IOException {
        if (Objects.isNull(inputStream)) {
            return false;
        }

        val definitions = JsonUtils.loadJsonFileFromStream(inputStream,
                                                           SignalDefinition::getName, SIGNAL_TYPE_REF);
        val newMap = getCjsSojDefinition(definitions);

        if (validator.test(newMap)) {
            updateAction.accept(newMap);
            return true;
        } else {
            return false;
        }
    }

    @NotNull
    private static Map<String, SignalDefinition> getCjsSojDefinition(Map<String, SignalDefinition> definitions) {
        val map = definitions
                .entrySet()
                .stream()
                .peek(entry -> entry
                        .getValue().getLogicalDefinition()
                        .removeIf(logicalDefinition ->
                                          !equalsIgnoreCase(logicalDefinition.getPlatform(), "SOJ")
                        )
                )
                .filter(entry -> !entry.getValue().getLogicalDefinition().isEmpty())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return ImmutableMap.copyOf(map);
    }
    // endregion

    // region validator provider
    private static SignalContext genCjsSyntaxValidationContext() {
        return new SignalContext(new RawEvent(), new UbiEvent());
    }

    private static boolean cjsSyntaxValidator(Map<String, SignalDefinition> metadata) {
        try {
            metadata.values().stream()
                    .map(SignalDefinition::getLogicalDefinition)
                    .flatMap(List::stream)
                    .forEach(x -> {
                        val uuidGen = x.getUuidGenerator().getFormula();

                        Objects.requireNonNull(JexlService.getExpression(uuidGen))
                               .evaluate(validationData.getJexlContext());

                        x.getEventClassifiers().forEach(ec -> {
                            val filter = ec.getFilter();
                            Objects.requireNonNull(JexlService.getExpression(filter))
                                   .evaluate(validationData.getJexlContext());
                        });
                    });

            return true;
        } catch (Throwable t) {
            // Any failure in the validation will be caught here
            log.error("CJS Metadata validation failed", t);
            return false;
        }
    }
    // endregion

    private InputStream getInputStream(String lkpType) {
        String fileName = getString(lkpType);
        Path filePath = new Path(lkpPath + fileName);
        return getInputStream(filePath);
    }

    private InputStream getInputStream(Path path) {
        InputStream instream = null;
        try {
            instream = getOrInitFS().open(path);
        } catch (Exception e) {
            log.warn("Load file failed from Filesystem [{}]", path, e);
        }
        return instream;
    }

    private InputStream getStreamFromClasspath(String resource) throws FileNotFoundException {
        InputStream instream;
        if (StringUtils.isNotBlank(resource)) {
            instream = CjsMetadataManager.class.getResourceAsStream(resource);

            if (instream == null) {
                throw new FileNotFoundException("Can't locate resource based on classPath: " + resource);
            }
        } else {
            throw new RuntimeException("Try to load empty resource.");
        }
        return instream;
    }

    public boolean isUpdate(String lkpType) {
        if (!initializedFlg) {
            return true;
        }

        String fileName = getString(lkpType);
        Path path = new Path(lkpPath + fileName);
        try {
            val fs = getOrInitFS();
            if (fs.exists(path)) {
                FileStatus[] fileStatus = fs.listStatus(path, new FileNameFilter(fileName));
                long fileModifiedTime = fileStatus[0].getModificationTime();
                long lastModifiedTime = lkpFileLastUpdDt.get(fileName) == null ? 0 : lkpFileLastUpdDt.get(fileName);
                boolean updated = fileModifiedTime > lastModifiedTime;
                if (updated) {
                    lkpFileLastFileSystemUpdDt.put(fileName, fileModifiedTime);
                }
                return updated;
            }
        } catch (Exception e) {
            log.error("Update Lkp File last UpdDt failed: {}[{}]", lkpType, path, e);
        } finally {
            closeFS();
        }
        return false;
    }

    public void closeFS() {
        if (fileSystem != null) {
            try {
                fileSystem.close();
            } catch (IOException e) {
                log.warn("Close Filesystem error.", e);
            } finally {
                fileSystem = null;
            }
        }
    }

    private FileSystem getOrInitFS() throws IOException, IllegalArgumentException {

        if (fileSystem == null) {
            Configuration configuration = new Configuration();
            configuration.setBoolean("fs.hdfs.impl.disable.cache", true);
            fileSystem = FileSystem.newInstance(configuration);
        }

        return fileSystem;
    }

    private void printLkpFileStatus() {
        StringBuilder stringBuilder = new StringBuilder();
        for (val entry : lkpFileLastUpdDt.entrySet()) {
            stringBuilder.append("Lkp FileName: ").append(entry.getKey()).append(", ");
            stringBuilder.append("Lkp File LastModifiedDate: ").append(entry.getValue()).append(";");
        }
        if (StringUtils.isNotEmpty(stringBuilder.toString())) {
            log.warn(stringBuilder.toString());
        }
    }

    private void updateLkpFileLastUpdDt(String lkpType) {
        String fileName = getString(lkpType);
        if (lkpFileLastFileSystemUpdDt.get(fileName) != null) {
            lkpFileLastUpdDt.put(fileName, lkpFileLastFileSystemUpdDt.get(fileName));
        }
    }

    @Override
    public synchronized void close() {
        if (Objects.nonNull(cjsMetadataManager)) {
            synchronized (CjsMetadataManager.class) {
                if (Objects.nonNull(cjsMetadataManager)) {
                    lkpRefreshTimeTask.cancel();
                    lkpFileLastFileSystemUpdDt.clear();
                    lkpFileLastUpdDt.clear();
                    closeFS();

                    cjsMetadataManager = null;
                }
            }
        }
    }

    private static class FileNameFilter implements PathFilter {

        private final String fileName;

        private FileNameFilter(String fileName) {
            this.fileName = fileName;
        }

        @Override
        public boolean accept(Path path) {
            return fileName.contains(path.getName());
        }
    }

}
