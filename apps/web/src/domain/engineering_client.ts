// Engineering record client for the TS Domain Store (P3-A).
//
// Same shape as the gateway engineering endpoints, in the same
// order: create/list/get/archive for each of the four record
// kinds (binding, project, location, worktree). Records are
// reference data only: no writes to disk, no Patch, no git.
//
// Server field names are snake_case; the domain shape is
// camelCase, so the mapping lives here, not in the view.
// Error handling follows effects_client.ts (HTTP status in
// the message), except the response body is appended when
// present, so a 409 archive refusal shows the server's own
// wording verbatim instead of only the status code.

export interface Binding {
  workspaceId: string;
  displayName: string;
  allowedRoots: string[];
  policyDigest: string;
  createdMs: number;
  updatedMs: number;
}

export interface Project {
  projectId: string;
  name: string;
  description: string;
  // Present when the gateway reports it (an archived project
  // reads back with status "archived"); absent on older rows.
  status: string | null;
}

export interface Location {
  locationId: string;
  workspaceId: string;
  name: string;
  canonicalRoot: string;
  rootSha256: string;
  status: string;
}

import {
  toEffectRecord,
  type EffectRecord,
  type ServerEffectRecord,
} from "./effects_client";

// Patch aggregate (P3-B, plan §4.1/§4.7). A Patch is one loop
// execution's change set in one worktree; a PatchFile is one
// proposed fs.write. The gateway computes the diff and both
// hashes at proposal time; `new_text` never leaves the
// gateway (skip_serializing), so there is no such field here.
// Status words are the gateway's stored words, verbatim:
// proposed | partially_applied | applied | rejected | stale
// for the Patch; pending | applied | failed | rejected |
// stale for a PatchFile.

export interface Patch {
  patchId: string;
  executionId: string;
  workspaceSnapshotId: string | null;
  worktreeId: string;
  projectId: string | null;
  status: string;
  createdMs: number;
  updatedMs: number | null;
}

export interface PatchFile {
  patchFileId: string;
  patchId: string;
  pathRelative: string;
  oldSha256: string | null;
  newSha256: string;
  diff: string;
  effectId: string;
  approvalId: string | null;
  status: string;
  createdMs: number;
  updatedMs: number | null;
}

export interface PatchDetail {
  patch: Patch;
  files: PatchFile[];
}

export interface PatchApplyResult {
  patchFile: PatchFile;
  effect: EffectRecord;
  replayed: boolean;
}

interface ServerPatch {
  patch_id: string;
  execution_id: string;
  workspace_snapshot_id?: string | null;
  worktree_id: string;
  project_id?: string | null;
  status: string;
  created_ms: number;
  updated_at_ms?: number | null;
}

interface ServerPatchFile {
  patch_file_id: string;
  patch_id: string;
  path_relative: string;
  old_sha256?: string | null;
  new_sha256: string;
  diff: string;
  effect_id: string;
  approval_id?: string | null;
  status: string;
  created_ms: number;
  updated_at_ms?: number | null;
}

interface ServerPatchDetail {
  patch: ServerPatch;
  files: ServerPatchFile[];
}

interface ServerPatchApplyResult {
  patch_file: ServerPatchFile;
  effect: ServerEffectRecord;
  replayed: boolean;
}

function toPatch(server: ServerPatch): Patch {
  return {
    patchId: server.patch_id,
    executionId: server.execution_id,
    workspaceSnapshotId: server.workspace_snapshot_id ?? null,
    worktreeId: server.worktree_id,
    projectId: server.project_id ?? null,
    status: server.status,
    createdMs: server.created_ms,
    updatedMs: server.updated_at_ms ?? null,
  };
}

function toPatchFile(server: ServerPatchFile): PatchFile {
  return {
    patchFileId: server.patch_file_id,
    patchId: server.patch_id,
    pathRelative: server.path_relative,
    oldSha256: server.old_sha256 ?? null,
    newSha256: server.new_sha256,
    diff: server.diff,
    effectId: server.effect_id,
    approvalId: server.approval_id ?? null,
    status: server.status,
    createdMs: server.created_ms,
    updatedMs: server.updated_at_ms ?? null,
  };
}

function toPatchDetail(server: ServerPatchDetail): PatchDetail {
  return {
    patch: toPatch(server.patch),
    files: server.files.map(toPatchFile),
  };
}

function toPatchApplyResult(server: ServerPatchApplyResult): PatchApplyResult {
  return {
    patchFile: toPatchFile(server.patch_file),
    effect: toEffectRecord(server.effect),
    replayed: server.replayed,
  };
}

export interface Worktree {
  worktreeId: string;
  locationId: string;
  directoryRelative: string;
  branchLabel: string;
  projectId: string | null;
  status: string;
}

interface ServerBinding {
  workspace_id: string;
  display_name: string;
  allowed_roots: string[];
  policy_digest: string;
  created_ms: number;
  updated_ms: number;
}

interface ServerProject {
  project_id: string;
  name: string;
  description: string;
  status?: string | null;
}

interface ServerLocation {
  location_id: string;
  workspace_id: string;
  name: string;
  canonical_root: string;
  root_sha256: string;
  status: string;
}

interface ServerWorktree {
  worktree_id: string;
  location_id: string;
  directory_relative: string;
  branch_label: string;
  project_id?: string | null;
  status: string;
}

function toBinding(server: ServerBinding): Binding {
  return {
    workspaceId: server.workspace_id,
    displayName: server.display_name,
    allowedRoots: server.allowed_roots,
    policyDigest: server.policy_digest,
    createdMs: server.created_ms,
    updatedMs: server.updated_ms,
  };
}

function toProject(server: ServerProject): Project {
  return {
    projectId: server.project_id,
    name: server.name,
    description: server.description,
    status: server.status ?? null,
  };
}

function toLocation(server: ServerLocation): Location {
  return {
    locationId: server.location_id,
    workspaceId: server.workspace_id,
    name: server.name,
    canonicalRoot: server.canonical_root,
    rootSha256: server.root_sha256,
    status: server.status,
  };
}

function toWorktree(server: ServerWorktree): Worktree {
  return {
    worktreeId: server.worktree_id,
    locationId: server.location_id,
    directoryRelative: server.directory_relative,
    branchLabel: server.branch_label,
    projectId: server.project_id ?? null,
    status: server.status,
  };
}

// Transport-neutral surface used by the view. The HTTP client
// below and the invoke client both implement it, so the view
// never knows which transport it runs on.
export interface EngineeringGateway {
  bindingCreate(displayName: string, path: string): Promise<Binding>;
  bindingList(): Promise<Binding[]>;
  bindingGet(workspaceId: string): Promise<Binding>;
  bindingArchive(workspaceId: string): Promise<Binding>;
  projectCreate(name: string, description: string): Promise<Project>;
  projectList(): Promise<Project[]>;
  projectGet(projectId: string): Promise<Project>;
  projectArchive(projectId: string): Promise<Project>;
  locationCreate(workspaceId: string, name: string, path: string): Promise<Location>;
  locationList(): Promise<Location[]>;
  locationGet(locationId: string): Promise<Location>;
  locationArchive(locationId: string): Promise<Location>;
  worktreeCreate(
    locationId: string,
    directoryRelative: string,
    branchLabel: string,
    projectId?: string | null,
  ): Promise<Worktree>;
  worktreeList(): Promise<Worktree[]>;
  worktreeGet(worktreeId: string): Promise<Worktree>;
  worktreeArchive(worktreeId: string): Promise<Worktree>;
  // Patches (P3-B): list (optional status filter), get with
  // files, apply one file (expected old hash + already-approved
  // approval), reject a whole Patch. No Patch-level approval
  // or Effect exists; approve goes through the existing
  // approval decide route, then apply consumes it.
  patchList(status?: string | null): Promise<Patch[]>;
  patchGet(patchId: string): Promise<PatchDetail>;
  patchApply(
    patchId: string,
    patchFileId: string,
    expectedOldSha256: string | null,
    approvalId: string,
  ): Promise<PatchApplyResult>;
  patchReject(patchId: string): Promise<PatchDetail>;
}

export class EngineeringClient implements EngineeringGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async request<T>(path: string, init?: RequestInit): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, init);
    if (!response.ok) {
      const text = (await response.text()).trim();
      throw new Error(
        text === ""
          ? `mock gateway returned HTTP ${response.status}`
          : `mock gateway returned HTTP ${response.status} ${text}`,
      );
    }
    return (await response.json()) as T;
  }

  private async post<T>(path: string, body: unknown): Promise<T> {
    return this.request<T>(path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  }

  async bindingCreate(displayName: string, path: string): Promise<Binding> {
    const record = await this.post<ServerBinding>("/bindings/create", {
      display_name: displayName,
      path,
    });
    return toBinding(record);
  }

  async bindingList(): Promise<Binding[]> {
    const records = await this.request<ServerBinding[]>("/bindings/list");
    return records.map(toBinding);
  }

  async bindingGet(workspaceId: string): Promise<Binding> {
    const record = await this.request<ServerBinding>(
      `/bindings/${encodeURIComponent(workspaceId)}`,
    );
    return toBinding(record);
  }

  async bindingArchive(workspaceId: string): Promise<Binding> {
    const record = await this.post<ServerBinding>(
      `/bindings/${encodeURIComponent(workspaceId)}/archive`,
      {},
    );
    return toBinding(record);
  }

  async projectCreate(name: string, description: string): Promise<Project> {
    const record = await this.post<ServerProject>("/projects/create", {
      name,
      description,
    });
    return toProject(record);
  }

  async projectList(): Promise<Project[]> {
    const records = await this.request<ServerProject[]>("/projects/list");
    return records.map(toProject);
  }

  async projectGet(projectId: string): Promise<Project> {
    const record = await this.request<ServerProject>(
      `/projects/${encodeURIComponent(projectId)}`,
    );
    return toProject(record);
  }

  async projectArchive(projectId: string): Promise<Project> {
    const record = await this.post<ServerProject>(
      `/projects/${encodeURIComponent(projectId)}/archive`,
      {},
    );
    return toProject(record);
  }

  async locationCreate(
    workspaceId: string,
    name: string,
    path: string,
  ): Promise<Location> {
    const record = await this.post<ServerLocation>("/locations/create", {
      workspace_id: workspaceId,
      name,
      path,
    });
    return toLocation(record);
  }

  async locationList(): Promise<Location[]> {
    const records = await this.request<ServerLocation[]>("/locations/list");
    return records.map(toLocation);
  }

  async locationGet(locationId: string): Promise<Location> {
    const record = await this.request<ServerLocation>(
      `/locations/${encodeURIComponent(locationId)}`,
    );
    return toLocation(record);
  }

  async locationArchive(locationId: string): Promise<Location> {
    const record = await this.post<ServerLocation>(
      `/locations/${encodeURIComponent(locationId)}/archive`,
      {},
    );
    return toLocation(record);
  }

  async worktreeCreate(
    locationId: string,
    directoryRelative: string,
    branchLabel: string,
    projectId?: string | null,
  ): Promise<Worktree> {
    const record = await this.post<ServerWorktree>("/worktrees/create", {
      location_id: locationId,
      directory_relative: directoryRelative,
      branch_label: branchLabel,
      project_id: projectId ?? null,
    });
    return toWorktree(record);
  }

  async worktreeList(): Promise<Worktree[]> {
    const records = await this.request<ServerWorktree[]>("/worktrees/list");
    return records.map(toWorktree);
  }

  async worktreeGet(worktreeId: string): Promise<Worktree> {
    const record = await this.request<ServerWorktree>(
      `/worktrees/${encodeURIComponent(worktreeId)}`,
    );
    return toWorktree(record);
  }

  async worktreeArchive(worktreeId: string): Promise<Worktree> {
    const record = await this.post<ServerWorktree>(
      `/worktrees/${encodeURIComponent(worktreeId)}/archive`,
      {},
    );
    return toWorktree(record);
  }

  async patchList(status?: string | null): Promise<Patch[]> {
    const query =
      status === null || status === undefined || status === ""
        ? "/patches/list"
        : `/patches/list?status=${encodeURIComponent(status)}`;
    const records = await this.request<ServerPatch[]>(query);
    return records.map(toPatch);
  }

  async patchGet(patchId: string): Promise<PatchDetail> {
    const detail = await this.request<ServerPatchDetail>(
      `/patches/${encodeURIComponent(patchId)}`,
    );
    return toPatchDetail(detail);
  }

  async patchApply(
    patchId: string,
    patchFileId: string,
    expectedOldSha256: string | null,
    approvalId: string,
  ): Promise<PatchApplyResult> {
    const result = await this.post<ServerPatchApplyResult>(
      `/patches/${encodeURIComponent(patchId)}/files/${encodeURIComponent(patchFileId)}/apply`,
      {
        expected_old_sha256: expectedOldSha256,
        approval_id: approvalId,
      },
    );
    return toPatchApplyResult(result);
  }

  async patchReject(patchId: string): Promise<PatchDetail> {
    const detail = await this.post<ServerPatchDetail>(
      `/patches/${encodeURIComponent(patchId)}/reject`,
      {},
    );
    return toPatchDetail(detail);
  }
}
