/*
 * Copyright 2020-2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.fhir.analytics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FhirEtlTest {

  @Test
  public void getServerUrls_singleUrl() {
    FhirEtlOptions options = PipelineOptionsFactory.as(FhirEtlOptions.class);
    options.setFhirServerUrl("http://localhost:8091/fhir");
    List<String> urls = FhirEtl.getServerUrls(options);
    assertThat(urls.size(), equalTo(1));
    assertThat(urls.get(0), equalTo("http://localhost:8091/fhir"));
  }

  @Test
  public void getServerUrls_multipleUrls() {
    FhirEtlOptions options = PipelineOptionsFactory.as(FhirEtlOptions.class);
    options.setFhirServerUrl(
        "http://server1:8091/fhir,http://server2:8091/fhir,http://server3:8091/fhir");
    List<String> urls = FhirEtl.getServerUrls(options);
    assertThat(urls.size(), equalTo(3));
    assertThat(urls.get(0), equalTo("http://server1:8091/fhir"));
    assertThat(urls.get(1), equalTo("http://server2:8091/fhir"));
    assertThat(urls.get(2), equalTo("http://server3:8091/fhir"));
  }

  @Test
  public void getServerUrls_multipleUrlsWithSpaces() {
    FhirEtlOptions options = PipelineOptionsFactory.as(FhirEtlOptions.class);
    options.setFhirServerUrl("http://server1:8091/fhir , http://server2:8091/fhir");
    List<String> urls = FhirEtl.getServerUrls(options);
    assertThat(urls.size(), equalTo(2));
    assertThat(urls.get(0), equalTo("http://server1:8091/fhir"));
    assertThat(urls.get(1), equalTo("http://server2:8091/fhir"));
  }

  @Test
  public void getServerUrls_emptyString() {
    FhirEtlOptions options = PipelineOptionsFactory.as(FhirEtlOptions.class);
    options.setFhirServerUrl("");
    List<String> urls = FhirEtl.getServerUrls(options);
    assertThat(urls.size(), equalTo(0));
  }
}
