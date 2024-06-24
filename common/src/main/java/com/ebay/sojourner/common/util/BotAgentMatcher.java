package com.ebay.sojourner.common.util;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class BotAgentMatcher {

  private static final int BOT_MATCH_THRESHOLD = 10000;
  // TODO: Use MinMaxPriorityQueue instead
  // Cache matched agents to reduce comparison.
  private Set<String> botAgents;
  private Filter<String> botAgentFilter;

  /**
   * Construct matcher wish provided browser agents and bot agents.
   */
  public BotAgentMatcher(Collection<String> browserAgents, Collection<String> botAgents) {
    this.botAgents = ConcurrentHashMap.newKeySet();
    this.botAgentFilter = new BotAgentFilter(browserAgents, botAgents);
  }

  /**
   * Do bot agent match for specific agent string
   *
   * @return true if it's a bot agent or false.
   */
  public boolean match(String agentString) throws IOException, InterruptedException {
    if (botAgents.contains(agentString)) {
      return true;
    } else if (botAgentFilter.filter(agentString)) {
      // Do not cache when cached size larger than threshold
      if (botAgents.size() < BOT_MATCH_THRESHOLD) {
        botAgents.add(agentString);
      }

      return true;
    } else {
      return false;
    }
  }

  /**
   * Clean cached agents which are coming from bots and agent string.
   */
  public void cleanup() throws IOException, InterruptedException {
    // release cached string earilier.
    this.botAgents.clear();
    this.botAgentFilter.cleanup();
  }
}
