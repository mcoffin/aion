package com.netscout.aion2;

import org.glassfish.jersey.server.ResourceConfig;

public class ResourceConfigUtils {
  /**
   * Registers an object with the resourceConfig
   *
   * @param cfg the ResourceConfig with which to register
   * @param obj the Object to register with the ResourceConfig
   */
  public static ResourceConfig register(ResourceConfig cfg, Object obj) {
    return cfg.register(obj);
  }
}
