"""2026-11-26 · Codex。只用合成资料与本机模型替身，观测实际模型请求。"""
import json
import os
import socket
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.request import Request, urlopen

binary = Path("target/debug/mock-gateway").resolve()
requests = []


class Model(BaseHTTPRequestHandler):
    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        requests.append(payload["messages"])
        reply = {"choices": [{"finish_reason": "stop", "message": {"role": "assistant", "content": "OK"}}]}
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(reply).encode())

    def log_message(self, *args):
        pass


def run():
    with tempfile.TemporaryDirectory() as temp, HTTPServer(("127.0.0.1", 0), Model) as model:
        threading.Thread(target=model.serve_forever, daemon=True).start()
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        origin = f"http://127.0.0.1:{port}"
        env = dict(PATH=os.defpath, DEEPSEEK_API_KEY="local-test", DEEPSEEK_BASE_URL=f"http://127.0.0.1:{model.server_port}",
                   MOCK_GATEWAY_ADDR=f"127.0.0.1:{port}", MOCK_SQLITE_PATH=f"{temp}/state.db", MOCK_FILES_ROOT=temp)
        proc = subprocess.Popen([binary], cwd=temp, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        def call(path, body=None):
            data = json.dumps(body).encode() if body is not None else None
            req = Request(origin + path, data=data, headers={"Content-Type": "application/json"})
            with urlopen(req, timeout=5) as response:
                return json.load(response)

        try:
            for attempt in range(50):
                try:
                    call("/health")
                    break
                except OSError:
                    if attempt == 49:
                        raise
                    time.sleep(0.1)
            pref = "preference-marker-67293"
            history = "conversation-marker-42107"
            question = "我的固定安排是什么？"
            call("/memories/save", {"layer": "user", "content": pref, "source": {"author": "test-user"}})
            assert any(row["content"] == pref for row in call("/memories/list"))
            first, second = (call("/sessions/create", {})["session_id"] for _ in range(2))
            for session, message in [(first, history), (first, question), (second, question)]:
                call("/sessions/send", {"session_id": session, "text": message})
            assert len(requests) == 3
            texts = [[msg["content"] for msg in request] for request in requests]
            print(json.dumps({"memory_read_back": True, "model_requests": len(texts),
                              "same_session_recalled_previous_turn": history in texts[1],
                              "fresh_session_received_only_question": texts[2] == [question],
                              "saved_memory_in_model_input": any(pref in text for group in texts for text in group)}, indent=2))
            assert history in texts[1] and texts[2] == [question]
            assert all(pref not in text for group in texts for text in group)
        finally:
            proc.terminate()
            proc.wait(timeout=10)
            model.shutdown()


if __name__ == "__main__":
    run()
