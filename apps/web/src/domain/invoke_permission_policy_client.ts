import { invoke } from "@tauri-apps/api/core";
import type {
  ActivePolicyPatch,
  ConnectorPolicy,
  EffectivePolicy,
  Hitl,
  Policy,
  PolicyDefaults,
  PolicyGateway,
  PolicyInventory,
  PolicySwitch,
  RevokeResult,
  ScopeRecord,
  SettingsPatch,
} from "./permission_policy_client";

// The Rust structs serialize snake_case; the domain shape is
// camelCase, so the mapping lives here, not in the view.
// Snooze through invoke: null means "no change" (Tauri
// deserializes JSON null into the outer None), a number sets.
// Clearing goes through deleteHitlSnooze, never through a
// PATCH null, so the invoke path never needs the third state.

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
    overrides: {
      hitlEnabled: server.overrides.hitl_enabled,
      mitmEnabled: server.overrides.mitm_enabled,
      tproxyEnabled: server.overrides.tproxy_enabled,
      sniRejectionEnabled: server.overrides.sni_rejection_enabled,
      hitlSnoozedUntilMs: server.overrides.hitl_snoozed_until_ms,
      updatedMs: server.overrides.updated_ms,
    },
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

export class InvokePermissionPolicyClient implements PolicyGateway {
  async getPolicy(): Promise<Policy> {
    return toPolicy(await invoke<ServerPolicy>("policy_get"));
  }

  async getActive(): Promise<EffectivePolicy> {
    return toEffectivePolicy(await invoke<ServerEffectivePolicy>("policy_active_get"));
  }

  async updateActive(patch: ActivePolicyPatch): Promise<EffectivePolicy> {
    const active = await invoke<ServerEffectivePolicy>("policy_active_update", {
      hitl_enabled: patch.hitlEnabled ?? null,
      mitm_enabled: patch.mitmEnabled ?? null,
      tproxy_enabled: patch.tproxyEnabled ?? null,
      sni_rejection_enabled: patch.sniRejectionEnabled ?? null,
      hitl_snoozed_until_ms: patch.snoozedUntilMs ?? null,
    });
    return toEffectivePolicy(active);
  }

  async updateConnector(connector: string, allowed: boolean): Promise<ConnectorPolicy> {
    const view = await invoke<ServerConnectorPolicy>("policy_connector_update", {
      connector,
      allowed,
    });
    return toConnectorPolicy(view);
  }

  async resetConnector(connector: string): Promise<ConnectorPolicy> {
    const view = await invoke<ServerConnectorPolicy>("policy_connector_reset", { connector });
    return toConnectorPolicy(view);
  }

  async updateHitl(enabled?: boolean, snoozedUntilMs?: number | null): Promise<Hitl> {
    const view = await invoke<ServerHitl>("policy_hitl_update", {
      enabled: enabled ?? null,
      snoozed_until_ms: snoozedUntilMs ?? null,
    });
    return toHitl(view);
  }

  async deleteHitlSnooze(): Promise<Hitl> {
    return toHitl(await invoke<ServerHitl>("policy_hitl_snooze_delete"));
  }

  async updateMitm(enabled: boolean): Promise<PolicySwitch> {
    return invoke<ServerPolicySwitch>("policy_mitm_update", { enabled });
  }

  async updateTproxy(enabled: boolean): Promise<PolicySwitch> {
    return invoke<ServerPolicySwitch>("policy_tproxy_update", { enabled });
  }

  async updateSniRejection(enabled: boolean): Promise<PolicySwitch> {
    return invoke<ServerPolicySwitch>("policy_sni_rejection_update", { enabled });
  }

  async updateNetworkRules(
    rules: { pattern: string; action: string }[],
  ): Promise<{ pattern: string; action: string }[]> {
    return invoke<ServerNetworkRule[]>("policy_network_rules_update", { rules });
  }

  async resetPolicy(): Promise<Policy> {
    return toPolicy(await invoke<ServerPolicy>("policy_reset"));
  }

  async revoke(scopeType?: string, scopeId?: string): Promise<RevokeResult> {
    return invoke<ServerRevokeResult>("policy_revoke", {
      scope_type: scopeType ?? null,
      scope_id: scopeId ?? null,
    });
  }

  async getScope(scopeType: string, scopeId: string): Promise<ScopeRecord> {
    const record = await invoke<ServerScopeRecord>("policy_scope_get", {
      scope_type: scopeType,
      scope_id: scopeId,
    });
    return toScopeRecord(record);
  }

  async updateScope(scopeType: string, scopeId: string, granted?: boolean): Promise<ScopeRecord> {
    const record = await invoke<ServerScopeRecord>("policy_scope_update", {
      scope_type: scopeType,
      scope_id: scopeId,
      granted: granted ?? null,
    });
    return toScopeRecord(record);
  }

  async getInventory(): Promise<PolicyInventory> {
    return toInventory(await invoke<ServerPolicyInventory>("policy_inventory"));
  }

  async getSettings(): Promise<PolicyDefaults> {
    return toPolicyDefaults(await invoke<ServerPolicyDefaults>("policy_settings_get"));
  }

  async updateSettings(patch: SettingsPatch): Promise<PolicyDefaults> {
    const defaults = await invoke<ServerPolicyDefaults>("policy_settings_update", {
      hitl_enabled: patch.hitlEnabled ?? null,
      mitm_enabled: patch.mitmEnabled ?? null,
      tproxy_enabled: patch.tproxyEnabled ?? null,
      sni_rejection_enabled: patch.sniRejectionEnabled ?? null,
    });
    return toPolicyDefaults(defaults);
  }

  async connectorPermissions(connector: string): Promise<ConnectorPolicy> {
    const view = await invoke<ServerConnectorPolicy>("policy_connector_permissions", { connector });
    return toConnectorPolicy(view);
  }
}
