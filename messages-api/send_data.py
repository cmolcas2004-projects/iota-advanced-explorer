import json, codecs, requests, os
from datetime import datetime, timezone
from flask import Flask, jsonify, request

app = Flask(__name__)
app.debug = True

TRACEABILITY_URL = os.environ.get(
    "TRACEABILITY_URL", "http://host.docker.internal:8000/messages"
)


@app.route('/upload', methods=['POST'])
def post_clear():
    node = "http://" + request.args.get("node") + ":14265/api/core/v2/blocks"
    requestData = request.get_json()
    tag = requestData["tag"]
    message = json.dumps(requestData["message"])

    tag_hex = "0x" + codecs.encode(tag, 'utf-8').hex()
    message_hex = "0x" + codecs.encode(message, 'utf-8').hex()

    payload = json.dumps({
        "protocolVersion": 2,
        "payload": {"type": 5, "tag": tag_hex, "data": message_hex}
    })
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        response = requests.request("POST", node, headers=headers, data=payload)
    except:
        return "Hornet node not found, check that the Hornet node exists.\n", 400

    print(response.status_code)
    print(response.text)

    if response.status_code in (200, 201, 202):
        try:
            block_id = response.json()["blockId"]
            requests.post(
                TRACEABILITY_URL,
                json={
                    "block_id": block_id,
                    "tag": tag,
                    "message": requestData["message"],
                    "sent_at": datetime.now(timezone.utc).isoformat(),
                },
                timeout=3,
            )
        except Exception as e:
            print(f"WARN: no se pudo avisar a trazabilidad: {e}")

    return jsonify(
        status_code=response.status_code,
        return_payload=response.text
    )


app.run(port=5555, host='0.0.0.0')
