/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.broker.broker;

import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.ws.rs.NotAuthorizedException;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.auth.BasicAuthPrincipal;
import org.apache.pinot.core.auth.BasicAuthUtils;
import org.apache.pinot.spi.auth.AuthorizationResult;
import org.apache.pinot.spi.auth.TableAuthorizationResult;
import org.apache.pinot.spi.auth.TableRowColAccessResult;
import org.apache.pinot.spi.auth.TableRowColAccessResultImpl;
import org.apache.pinot.spi.auth.broker.RequesterIdentity;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Basic Authentication based on http headers. Configured via the "pinot.broker.access.control" family of properties.
 *
 * <pre>
 *     Example:
 *     pinot.broker.access.control.principals=admin123,user456
 *     pinot.broker.access.control.principals.admin123.password=verysecret
 *     pinot.broker.access.control.principals.user456.password=kindasecret
 *     pinot.broker.access.control.principals.user456.tables=stuff,lessImportantStuff
 * </pre>
 */
public class BasicAuthAccessControlFactory extends AccessControlFactory {
  private static final String PREFIX = "principals";

  private AccessControl _accessControl;

  public BasicAuthAccessControlFactory() {
    // left blank
  }

  @Override
  public void init(PinotConfiguration configuration) {
    _accessControl = new BasicAuthAccessControl(BasicAuthUtils.extractBasicAuthPrincipals(configuration, PREFIX));
  }

  @Override
  public AccessControl create() {
    return _accessControl;
  }

  /**
   * Access Control using header-based basic http authentication
   */
  private static class BasicAuthAccessControl implements AccessControl {
    private final Map<String, BasicAuthPrincipal> _token2principal;

    public BasicAuthAccessControl(Collection<BasicAuthPrincipal> principals) {
      _token2principal = principals.stream().collect(Collectors.toMap(BasicAuthPrincipal::getToken, p -> p));
    }

    @Override
    public AuthorizationResult authorize(RequesterIdentity requesterIdentity) {
      return authorize(requesterIdentity, (BrokerRequest) null);
    }

    @Override
    public AuthorizationResult authorize(RequesterIdentity requesterIdentity, BrokerRequest brokerRequest) {
      Optional<BasicAuthPrincipal> principalOpt = getPrincipalOpt(requesterIdentity);

      if (!principalOpt.isPresent()) {
        throw new NotAuthorizedException("Basic");
      }

      BasicAuthPrincipal principal = principalOpt.get();
      if (brokerRequest == null || !brokerRequest.isSetQuerySource() || !brokerRequest.getQuerySource()
          .isSetTableName()) {
        // no table restrictions? accept
        return TableAuthorizationResult.success();
      }

      Set<String> failedTables = new HashSet<>();

      if (!principal.hasTable(brokerRequest.getQuerySource().getTableName())) {
        failedTables.add(brokerRequest.getQuerySource().getTableName());
      }
      if (failedTables.isEmpty()) {
        return TableAuthorizationResult.success();
      }
      return new TableAuthorizationResult(failedTables);
    }

    @Override
    public TableAuthorizationResult authorize(RequesterIdentity requesterIdentity, Set<String> tables) {
      Optional<BasicAuthPrincipal> principalOpt = getPrincipalOpt(requesterIdentity);

      if (!principalOpt.isPresent()) {
        throw new NotAuthorizedException("Basic");
      }

      if (tables == null || tables.isEmpty()) {
        return TableAuthorizationResult.success();
      }
      BasicAuthPrincipal principal = principalOpt.get();
      Set<String> failedTables = new HashSet<>();
      for (String table : tables) {
        if (!principal.hasTable(table)) {
          failedTables.add(table);
        }
      }
      if (failedTables.isEmpty()) {
        return TableAuthorizationResult.success();
      }
      return new TableAuthorizationResult(failedTables);
    }

    @Override
    public TableRowColAccessResult getRowColFilters(RequesterIdentity requesterIdentity, String table) {
      Optional<BasicAuthPrincipal> principalOpt = getPrincipalOpt(requesterIdentity);

      Preconditions.checkState(principalOpt.isPresent(), "Principal is not authorized");
      Preconditions.checkState(table != null, "Table cannot be null");

      TableRowColAccessResult tableRowColAccessResult = new TableRowColAccessResultImpl();
      BasicAuthPrincipal principal = principalOpt.get();

      //precondition: The principal should have the table.
      Preconditions.checkArgument(principal.hasTable(table),
          "Principal: " + principal.getName() + " does not have access to table: " + table);

      Optional<List<String>> rlsFiltersMaybe = principal.getRLSFilters(table);
      rlsFiltersMaybe.ifPresent(tableRowColAccessResult::setRLSFilters);

      return tableRowColAccessResult;
    }

    private Optional<BasicAuthPrincipal> getPrincipalOpt(RequesterIdentity requesterIdentity) {
      Collection<String> tokens = extractAuthorizationTokens(requesterIdentity);
      if (tokens.isEmpty()) {
        return Optional.empty();
      }
      return tokens.stream().map(org.apache.pinot.common.auth.BasicAuthUtils::normalizeBase64Token)
          .map(_token2principal::get).filter(Objects::nonNull)
          .findFirst();
    }
  }
}
