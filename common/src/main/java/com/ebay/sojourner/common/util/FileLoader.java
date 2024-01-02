package com.ebay.sojourner.common.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class FileLoader {

  public static String loadContent(String absoluteFilePath, String alternativeResource) {
    String content = null;
    InputStream in = null;
    try {
      in = loadInStream(absoluteFilePath, alternativeResource);
      StringBuilder resultBuilder = new StringBuilder();
      byte[] bytes = new byte[4096];
      int readBytes = 0;

      while ((readBytes = in.read(bytes)) != -1) {
        resultBuilder.append(new String(Arrays.copyOfRange(bytes, 0, readBytes),
            StandardCharsets.UTF_8));
        bytes = new byte[4096];
      }
      content = resultBuilder.toString().trim();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException ex) {
          ex.printStackTrace();
        }
      }
    }

    return content;
  }

  public static String loadContent(InputStream fileName) {
    String content = null;
    InputStream in = null;
    try {
      in = loadInStream(fileName);
      //            in = FileLoader.class.getResourceAsStream(fileName.toString());
      StringBuilder resultBuilder = new StringBuilder();
      byte[] bytes = new byte[4096];
      int readBytes = 0;

      while ((readBytes = in.read(bytes)) != -1) {
        resultBuilder.append(new String(Arrays.copyOfRange(bytes, 0, readBytes),
            StandardCharsets.UTF_8));
        bytes = new byte[4096];
      }
      content = resultBuilder.toString().trim();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException ex) {
          ex.printStackTrace();
        }
      }
    }

    return content;
  }

  @SuppressWarnings("resource")
  public static InputStream loadInStream(String path, String resource)
      throws FileNotFoundException {
    InputStream instream = null;
    try {
      if (StringUtils.isNotBlank(path)) {
        //                instream = new FileInputStream(path);
        instream = FileLoader.class.getResourceAsStream(path);
      } else {
        log.info("Load resource directly as provided path is empty, resource: " + resource);
        instream = loadResource(resource);
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      log.info(
          "Load file failed from path: " + path + ", and try to load from resource: " + resource);
      instream = loadResource(resource);
    }

    return instream;
  }

  @SuppressWarnings("resource")
  public static InputStream loadInStream(InputStream fileStream) throws FileNotFoundException {
    InputStream instream = null;
    try {
      if (fileStream != null) {
        //                instream = new FileInputStream(file);
        instream = fileStream;
      } else {
        log.info("file is null");
      }
    } catch (Exception e) {
      e.printStackTrace();
      log.info("Load file failed ");
    }

    return instream;
  }

  private static InputStream loadResource(String resource) throws FileNotFoundException {
    InputStream instream = null;
    if (StringUtils.isNotBlank(resource)) {
      instream = FileLoader.class.getResourceAsStream(resource);
      if (instream == null) {
        throw new FileNotFoundException("Can't locate resource based on classPath: " + resource);
      }
    } else {
      throw new RuntimeException("Try to load empty resource: " + resource);
    }

    return instream;
  }
}
