// Permission policy client for the TS Domain Store (Task 11):
// defaults, active overrides, hitl/snooze, switches, network
// rules, per-connector policies, scopes, reset, revoke, and
// inventory.
//
// Same shape as the vault client: the gateway returns
// snake_case; the domain shape is camelCase, so the mapping
// lives here, not in the view. These calls carry no approval
// loop (Task 9/10 decision). Nothing here enforces a policy:
// the gateway stores policy data only, and the switches do not
// intercept real traffic — the Settings view says so plainly.
//
// Snooze is three-state on the wire: an omitted key means "no
// change", explicit null clears, a number sets. JSON.stringify
// drops undefined-valued keys, so the methods below pass
// `number | null | undefined` straight through.

export interface PolicyDefaults {
  hitlEnabled: boolean;
  mitmEnabled: boolean;
  tproxyEnabled: boolean;
  sniRejectionEnabled: boolean;
  updatedMs: number;
}

export interface EffectivePolicy {
  hitlEnabled: boolean;
  mitmEnabled: boolean;
  tproxyEnabled: boolean;
  sniRejectionEnabled: boolean;
  hitlSnoozedUntilMs: number | null;
  updatedMs: number;
}

export interface PolicyOverrides {
  hitlEnabled: boolean | null;
  mitmEnabled: boolean | null;
  tproxyEnabled: boolean | null;
  sniRejectionEnabled: boolean | null;
  hitlSnoozedUntilMs: number | null;
  updatedMs: number;
}

export interface ConnectorPolicy {
  connectorId: string;
  allowed: boolean;
  updatedMs: number;
}

export interface NetworkRule {
  pattern: string;
  action: string;
}

export interface ScopeRecord {
  scopeType: string;
  scopeId: string;
  granted: boolean;
  updatedMs: number;
}

export interface Policy {
  defaults: PolicyDefaults;
  active: EffectivePolicy;
  overrides: PolicyOverrides;
  connectors: ConnectorPolicy[];
  scopes: ScopeRecord[];
  networkRules: NetworkRule[];
}

export interface Hitl {
  enabled: boolean;
  snoozedUntilMs: number | null;
}

export interface PolicySwitch {
  enabled: boolean;
}

export interface PolicyInventory {
  sections: string[];
  connectorPolicies: number;
  scopes: number;
  networkRules: number;
  defaultsCustomized: boolean;
}

export interface RevokeResult {
  revoked: string[];
}

interface ServerPolicyDefaults {
  hitl_enabled: boolean;
  mitm_enabled: boolean;
  tproxy_enabled: boolean;
  sni_rejection_enabled: boolean;
  updated_ms: number;
}

interface ServerEffectivePolicy {
  hitl_enabled: boolean;
  mitm_enabled: boolean;
  tproxy_enabled: boolean;
  sni_rejection_enabled: boolean;
  hitl_snoozed_until_ms: number | null;
  updated_ms: number;
}

interface ServerPolicyOverrides {
  hitl_enabled: boolean | null;
  mitm_enabled: boolean | null;
  tproxy_enabled: boolean | null;
  sni_rejection_enabled: boolean | null;
  hitl_snoozed_until_ms: number | null;
  updated_ms: number;
}

interface ServerConnectorPolicy {
  connector_id: string;
  allowed: boolean;
  updated_ms: number;
}

interface ServerNetworkRule {
  pattern: string;
  action: string;
}

interface ServerScopeRecord {
  scope_type: string;
  scope_id: string;
  granted: boolean;
  updated_ms: number;
}

interface ServerPolicy {
  defaults: ServerPolicyDefaults;
  active: ServerEffectivePolicy;
  overrides: ServerPolicyOverrides;
  connectors: ServerConnectorPolicy[];
  scopes: ServerScopeRecord[];
  network_rules: ServerNetworkRule[];
}

interface ServerHitl {
  enabled: boolean;
  snoozed_until_ms: number | null;
}

interface ServerPolicySwitch {
  enabled: boolean;
}

interface ServerPolicyInventory {
  sections: string[];
  connector_policies: number;
  scopes: number;
  network_rules: number;
  defaults_customized: boolean;
}

interface ServerRevokeResult {
  revoked: string[];
}

function toPolicyDefaults(server: ServerPolicyDefaults): PolicyDefaults {
  return {
    hitlEnabled: server.hitl_enabled,
    mitmEnabled: server.mitm_enabled,
    tproxyEnabled: server.tproxy_enabled,
    sniRejectionEnabled: server.sni_rejection_enabled,
    updatedMs: server.updated_ms,
  };
}

function toEffectivePolicy(server: ServerEffectivePolicy): EffectivePolicy {
  return {
    hitlEnabled: server.hitl_enabled,
    mitmEnabled: server.mitm_enabled,
    tproxyEnabled: server.tproxy_enabled,
    sniRejectionEnabled: server.sni_rejection_enabled,
    hitlSnoozedUntilMs: server.hitl_snoozed_until_ms,
    updatedMs: server.updated_ms,
  };
}

function toPolicyOverrides(server: ServerPolicyOverrides): PolicyOverrides {
  return {
    hitlEnabled: server.hitl_enabled,
    mitmEnabled: server.mitm_enabled,
    tproxyEnabled: server.tproxy_enabled,
    sniRejectionEnabled: server.sni_rejection_enabled,
    hitlSnoozedUntilMs: server.hitl_snoozed_until_ms,
    updatedMs: server.updated_ms,
  };
}

function toConnectorPolicy(server: ServerConnectorPolicy): ConnectorPolicy {
  return {
    connectorId: server.connector_id,
    allowed: server.allowed,
    updatedMs: server.updated_ms,
  };
}

function toScopeRecord(server: ServerScopeRecord): ScopeRecord {
  return {
    scopeType: server.scope_type,
    scopeId: server.scope_id,
    granted: server.granted,
    updatedMs: server.updated_ms,
  };
}

function toPolicy(server: ServerPolicy): Policy {
  return {
    defaults: toPolicyDefaults(server.defaults),
    active: toEffectivePolicy(server.active),
    overrides: toPolicyOverrides(server.overrides),
    connectors: server.connectors.map(toConnectorPolicy),
    scopes: server.scopes.map(toScopeRecord),
    networkRules: server.network_rules.map((r) => ({ pattern: r.pattern, action: r.action })),
  };
}

function toHitl(server: ServerHitl): Hitl {
  return { enabled: server.enabled, snoozedUntilMs: server.snoozed_until_ms };
}

function toInventory(server: ServerPolicyInventory): PolicyInventory {
  return {
    sections: server.sections,
    connectorPolicies: server.connector_policies,
    scopes: server.scopes,
    networkRules: server.network_rules,
    defaultsCustomized: server.defaults_customized,
  };
}

export interface ActivePolicyPatch {
  hitlEnabled?: boolean;
  mitmEnabled?: boolean;
  tproxyEnabled?: boolean;
  sniRejectionEnabled?: boolean;
  snoozedUntilMs?: number | null;
}

export interface SettingsPatch {
  hitlEnabled?: boolean;
  mitmEnabled?: boolean;
  tproxyEnabled?: boolean;
  sniRejectionEnabled?: boolean;
}

// Transport-neutral surface used by the Settings view. The HTTP
// client below and the invoke client both implement it, so the
// view never knows which transport it runs on.
export interface PolicyGateway {
  getPolicy(): Promise<Policy>;
  getActive(): Promise<EffectivePolicy>;
  updateActive(patch: ActivePolicyPatch): Promise<EffectivePolicy>;
  updateConnector(connector: string, allowed: boolean): Promise<ConnectorPolicy>;
  resetConnector(connector: string): Promise<ConnectorPolicy>;
  updateHitl(enabled?: boolean, snoozedUntilMs?: number | null): Promise<Hitl>;
  deleteHitlSnooze(): Promise<Hitl>;
  updateMitm(enabled: boolean): Promise<PolicySwitch>;
  updateTproxy(enabled: boolean): Promise<PolicySwitch>;
  updateSniRejection(enabled: boolean): Promise<PolicySwitch>;
  updateNetworkRules(rules: NetworkRule[]): Promise<NetworkRule[]>;
  resetPolicy(): Promise<Policy>;
  revoke(scopeType?: string, scopeId?: string): Promise<RevokeResult>;
  getScope(scopeType: string, scopeId: string): Promise<ScopeRecord>;
  updateScope(scopeType: string, scopeId: string, granted?: boolean): Promise<ScopeRecord>;
  getInventory(): Promise<PolicyInventory>;
  getSettings(): Promise<PolicyDefaults>;
  updateSettings(patch: SettingsPatch): Promise<PolicyDefaults>;
  connectorPermissions(connector: string): Promise<ConnectorPolicy>;
}

const ALLOWED_HOSTS = ["127.0.0.1", "localhost"];

function checkUrl(baseUrl: string): void {
  const stripped = baseUrl.replace(/^http:\/\//, "").replace(/\/$/, "");
  const host = stripped.split(":")[0];
  if (!baseUrl.startsWith("http://") || !ALLOWED_HOSTS.includes(host)) {
    throw new Error("egress refused: destination is not on the Mock allowlist");
  }
}

export class PermissionPolicyClient implements PolicyGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    checkUrl(baseUrl);
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method,
      headers: { "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!response.ok) {
      throw new Error(`mock gateway returned HTTP ${response.status}`);
    }
    return (await response.json()) as T;
  }

  private async get<T>(path: string): Promise<T> {
    return this.request<T>("GET", path);
  }

  private async patch<T>(path: string, body: unknown): Promise<T> {
    return this.request<T>("PATCH", path, body);
  }

  private async post<T>(path: string, body: unknown): Promise<T> {
    return this.request<T>("POST", path, body);
  }

  private async del<T>(path: string): Promise<T> {
    return this.request<T>("DELETE", path);
  }

  async getPolicy(): Promise<Policy> {
    return toPolicy(await this.get<ServerPolicy>("/permissions"));
  }

  async getActive(): Promise<EffectivePolicy> {
    return toEffectivePolicy(await this.get<ServerEffectivePolicy>("/permissions/active"));
  }

  async updateActive(patch: ActivePolicyPatch): Promise<EffectivePolicy> {
    const active = await this.patch<ServerEffectivePolicy>("/permissions/active", {
      hitl_enabled: patch.hitlEnabled ?? null,
      mitm_enabled: patch.mitmEnabled ?? null,
      tproxy_enabled: patch.tproxyEnabled ?? null,
      sni_rejection_enabled: patch.sniRejectionEnabled ?? null,
      hitl_snoozed_until_ms: patch.snoozedUntilMs,
    });
    return toEffectivePolicy(active);
  }

  async updateConnector(connector: string, allowed: boolean): Promise<ConnectorPolicy> {
    const view = await this.patch<ServerConnectorPolicy>(
      `/permissions/connectors/${encodeURIComponent(connector)}`,
      { allowed },
    );
    return toConnectorPolicy(view);
  }

  async resetConnector(connector: string): Promise<ConnectorPolicy> {
    const view = await this.del<ServerConnectorPolicy>(
      `/permissions/connectors/${encodeURIComponent(connector)}`,
    );
    return toConnectorPolicy(view);
  }

  async updateHitl(enabled?: boolean, snoozedUntilMs?: number | null): Promise<Hitl> {
    const view = await this.patch<ServerHitl>("/permissions/hitl", {
      enabled: enabled ?? null,
      snoozed_until_ms: snoozedUntilMs,
    });
    return toHitl(view);
  }

  async deleteHitlSnooze(): Promise<Hitl> {
    return toHitl(await this.del<ServerHitl>("/permissions/hitl/snooze"));
  }

  async updateMitm(enabled: boolean): Promise<PolicySwitch> {
    return this.patch<ServerPolicySwitch>("/permissions/mitm", { enabled });
  }

  async updateTproxy(enabled: boolean): Promise<PolicySwitch> {
    return this.patch<ServerPolicySwitch>("/permissions/tproxy", { enabled });
  }

  async updateSniRejection(enabled: boolean): Promise<PolicySwitch> {
    return this.patch<ServerPolicySwitch>("/permissions/sni-rejection", { enabled });
  }

  async updateNetworkRules(rules: NetworkRule[]): Promise<NetworkRule[]> {
    const updated = await this.patch<ServerNetworkRule[]>("/permissions/network-rules", { rules });
    return updated.map((r) => ({ pattern: r.pattern, action: r.action }));
  }

  async resetPolicy(): Promise<Policy> {
    return toPolicy(await this.post<ServerPolicy>("/permissions/reset", {}));
  }

  async revoke(scopeType?: string, scopeId?: string): Promise<RevokeResult> {
    return this.post<ServerRevokeResult>("/permissions/revoke", {
      scope_type: scopeType ?? null,
      scope_id: scopeId ?? null,
    });
  }

  async getScope(scopeType: string, scopeId: string): Promise<ScopeRecord> {
    const record = await this.get<ServerScopeRecord>(
      `/permissions/scopes/${encodeURIComponent(scopeType)}/${encodeURIComponent(scopeId)}`,
    );
    return toScopeRecord(record);
  }

  async updateScope(scopeType: string, scopeId: string, granted?: boolean): Promise<ScopeRecord> {
    const record = await this.patch<ServerScopeRecord>(
      `/permissions/scopes/${encodeURIComponent(scopeType)}/${encodeURIComponent(scopeId)}`,
      { granted: granted ?? null },
    );
    return toScopeRecord(record);
  }

  async getInventory(): Promise<PolicyInventory> {
    return toInventory(await this.get<ServerPolicyInventory>("/permissions/inventory"));
  }

  async getSettings(): Promise<PolicyDefaults> {
    return toPolicyDefaults(await this.get<ServerPolicyDefaults>("/permissions/settings"));
  }

  async updateSettings(patch: SettingsPatch): Promise<PolicyDefaults> {
    const defaults = await this.patch<ServerPolicyDefaults>("/permissions/settings", {
      hitl_enabled: patch.hitlEnabled ?? null,
      mitm_enabled: patch.mitmEnabled ?? null,
      tproxy_enabled: patch.tproxyEnabled ?? null,
      sni_rejection_enabled: patch.sniRejectionEnabled ?? null,
    });
    return toPolicyDefaults(defaults);
  }

  async connectorPermissions(connector: string): Promise<ConnectorPolicy> {
    const view = await this.get<ServerConnectorPolicy>(
      `/connectors/${encodeURIComponent(connector)}/permissions`,
    );
    return toConnectorPolicy(view);
  }
}
