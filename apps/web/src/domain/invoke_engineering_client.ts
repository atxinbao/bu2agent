import { invoke } from "@tauri-apps/api/core";
import type {
  Binding,
  EngineeringGateway,
  Location,
  Project,
  Worktree,
} from "./engineering_client";

// The Rust engineering record serializes snake_case; the domain
// shape is camelCase, so the mapping lives here, not in the
// view.
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

// Engineering line over Tauri invoke, backed by the Rust
// EngineeringClient. Invoke argument keys are snake_case to
// match the Rust command parameters exactly.
//
// The engineering_* Tauri commands landed on the Rust side
// (apps/shell/src-tauri/src/tool_commands.rs) with matching
// snake_case names and argument keys; re-check against them
// if either side changes.
export class InvokeEngineeringClient implements EngineeringGateway {
  async bindingCreate(displayName: string, path: string): Promise<Binding> {
    const record = await invoke<ServerBinding>("engineering_binding_create", {
      display_name: displayName,
      path,
    });
    return toBinding(record);
  }

  async bindingList(): Promise<Binding[]> {
    const records = await invoke<ServerBinding[]>("engineering_binding_list");
    return records.map(toBinding);
  }

  async bindingGet(workspaceId: string): Promise<Binding> {
    const record = await invoke<ServerBinding>("engineering_binding_get", {
      workspace_id: workspaceId,
    });
    return toBinding(record);
  }

  async bindingArchive(workspaceId: string): Promise<Binding> {
    const record = await invoke<ServerBinding>("engineering_binding_archive", {
      workspace_id: workspaceId,
    });
    return toBinding(record);
  }

  async projectCreate(name: string, description: string): Promise<Project> {
    const record = await invoke<ServerProject>("engineering_project_create", {
      name,
      description,
    });
    return toProject(record);
  }

  async projectList(): Promise<Project[]> {
    const records = await invoke<ServerProject[]>("engineering_project_list");
    return records.map(toProject);
  }

  async projectGet(projectId: string): Promise<Project> {
    const record = await invoke<ServerProject>("engineering_project_get", {
      project_id: projectId,
    });
    return toProject(record);
  }

  async projectArchive(projectId: string): Promise<Project> {
    const record = await invoke<ServerProject>("engineering_project_archive", {
      project_id: projectId,
    });
    return toProject(record);
  }

  async locationCreate(
    workspaceId: string,
    name: string,
    path: string,
  ): Promise<Location> {
    const record = await invoke<ServerLocation>("engineering_location_create", {
      workspace_id: workspaceId,
      name,
      path,
    });
    return toLocation(record);
  }

  async locationList(): Promise<Location[]> {
    const records = await invoke<ServerLocation[]>("engineering_location_list");
    return records.map(toLocation);
  }

  async locationGet(locationId: string): Promise<Location> {
    const record = await invoke<ServerLocation>("engineering_location_get", {
      location_id: locationId,
    });
    return toLocation(record);
  }

  async locationArchive(locationId: string): Promise<Location> {
    const record = await invoke<ServerLocation>("engineering_location_archive", {
      location_id: locationId,
    });
    return toLocation(record);
  }

  async worktreeCreate(
    locationId: string,
    directoryRelative: string,
    branchLabel: string,
    projectId?: string | null,
  ): Promise<Worktree> {
    const record = await invoke<ServerWorktree>("engineering_worktree_create", {
      location_id: locationId,
      directory_relative: directoryRelative,
      branch_label: branchLabel,
      project_id: projectId ?? null,
    });
    return toWorktree(record);
  }

  async worktreeList(): Promise<Worktree[]> {
    const records = await invoke<ServerWorktree[]>("engineering_worktree_list");
    return records.map(toWorktree);
  }

  async worktreeGet(worktreeId: string): Promise<Worktree> {
    const record = await invoke<ServerWorktree>("engineering_worktree_get", {
      worktree_id: worktreeId,
    });
    return toWorktree(record);
  }

  async worktreeArchive(worktreeId: string): Promise<Worktree> {
    const record = await invoke<ServerWorktree>("engineering_worktree_archive", {
      worktree_id: worktreeId,
    });
    return toWorktree(record);
  }
}
