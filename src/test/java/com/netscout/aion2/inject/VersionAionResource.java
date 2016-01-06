package com.netscout.aion2.inject;

import java.io.Serializable;
import java.lang.annotation.Annotation;

public class VersionAionResource implements AionResource, Serializable {
  @Override
  public String resourcePath() {
    return "version.properties";
  }

  public Class<? extends Annotation> annotationType() {
    return AionResource.class;
  }

  public int hashCode() {
    return (127 * "resourcePath".hashCode()) ^ resourcePath().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof AionResource)) {
      return false;
    }
    return toString().equals(other.toString());
  }

  public String toString() {
    return "@" + AionResource.class.getName() + "(resourcePath=" + resourcePath() + ")";
  }

  private static final long serialVersionUID = 0;
}
