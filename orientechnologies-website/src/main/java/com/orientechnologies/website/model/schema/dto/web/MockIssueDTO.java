package com.orientechnologies.website.model.schema.dto.web;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by Enrico Risa on 24/06/15.
 */

@JsonIgnoreProperties(ignoreUnknown = true)
public class MockIssueDTO extends IssueDTO {

  @JsonProperty("client")
  public Integer getClientId() {
    return super.getClient();
  }

}
