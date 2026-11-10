import { useEffect, useState } from "react";
import type { Goal, GoalGateway } from "../domain/goals_client";

// Goals view over the GoalGateway interface (D1-1, minimal
// list/detail). A goal is a first-class object a task can
// link to (goal_id). Archive is a status flip, never a
// delete: the list hides archived rows, detail still reads
// them. No Effect, no approval — direct management actions,
// same as Project.
export function GoalsView({ client }: { client: GoalGateway }) {
  const [goals, setGoals] = useState<Goal[]>([]);
  const [selected, setSelected] = useState<Goal | null>(null);
  const [title, setTitle] = useState("");
  const [description, setDescription] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  async function refresh() {
    setBusy(true);
    setError(null);
    try {
      setGoals(await client.listGoals());
    } catch (err) {
      setError(err instanceof Error ? err.message : "list failed");
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    void refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function run(action: () => Promise<unknown>, done: string) {
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await action();
      setNotice(done);
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "action failed");
    } finally {
      setBusy(false);
    }
  }

  async function select(goalId: string) {
    setBusy(true);
    setError(null);
    try {
      setSelected(await client.getGoal(goalId));
    } catch (err) {
      setError(err instanceof Error ? err.message : "get failed");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div>
      <h2>Goals</h2>
      <p className="note">
        目标是一等对象：Task 可挂 goal_id。归档是状态翻转，永不删除；列表隐藏已归档行，详情仍可读。不建
        Effect、不走审批，与 Project 同款直接管理动作。
      </p>
      {error !== null && <p className="error">{error}</p>}
      {notice !== null && <p className="note">{notice}</p>}
      <div>
        <input
          value={title}
          onChange={(event) => setTitle(event.target.value)}
          placeholder="Title"
          disabled={busy}
        />
        <input
          value={description}
          onChange={(event) => setDescription(event.target.value)}
          placeholder="Description (optional)"
          disabled={busy}
        />
        <button
          type="button"
          disabled={busy || title.trim() === ""}
          onClick={() =>
            void run(
              async () => {
                await client.createGoal(
                  title.trim(),
                  description.trim() === "" ? undefined : description.trim(),
                );
                setTitle("");
                setDescription("");
              },
              "Goal created.",
            )
          }
        >
          Create goal
        </button>
        <button type="button" disabled={busy} onClick={() => void refresh()}>
          Refresh
        </button>
      </div>
      {goals.length === 0 ? (
        <p className="empty">No active goals yet.</p>
      ) : (
        <ul>
          {goals.map((goal) => (
            <li key={goal.goalId}>
              <button type="button" disabled={busy} onClick={() => void select(goal.goalId)}>
                {goal.title} ({goal.goalId}, {goal.status})
              </button>
            </li>
          ))}
        </ul>
      )}
      {selected !== null && (
        <div>
          <h3>{selected.title}</h3>
          <p>
            {selected.goalId} · {selected.status}
          </p>
          {selected.description !== "" && <p>{selected.description}</p>}
          <p className="note">
            created {new Date(selected.createdMs).toLocaleString()} · origin{" "}
            {selected.originDeviceId}
          </p>
          {selected.status === "active" && (
            <button
              type="button"
              disabled={busy}
              onClick={() =>
                void run(
                  async () => {
                    setSelected(await client.archiveGoal(selected.goalId));
                  },
                  "Goal archived.",
                )
              }
            >
              Archive
            </button>
          )}
        </div>
      )}
    </div>
  );
}
