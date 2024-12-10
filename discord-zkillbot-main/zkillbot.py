import asyncio
import websockets
import json
import requests
import time

DISCORD_WEBHOOK_URL = ""

async def receive_messages(websocket_url, subscription_payload):
    while True:
        try:
            async with websockets.connect(websocket_url) as websocket:
                print(f"Connected to WebSocket: {websocket_url}")
                
                # Send the subscription payload
                await websocket.send(json.dumps(subscription_payload))
                print(f"Sent payload: {subscription_payload}")

                # Start receiving messages
                while True:
                    message = await websocket.recv()
                    print(f"Raw message received: {message}")
                    try:
                        # Attempt to parse the message as JSON
                        data = json.loads(message)
                        print(f"Parsed JSON: {json.dumps(data, indent=4)}")

                        # Extract required fields from the 'zkb' object
                        zkb_data = data.get("zkb", {})
                        total_value = zkb_data.get("totalValue")
                        url = zkb_data.get("url")

                        # Check if the required fields are present and meet the condition
                        if total_value is not None and url and total_value > 10_000_000_000:
                            print(f"Filtered Message - URL: {url}")
                            
                            # Send the filtered message to Discord webhook
                            discord_payload = {
                                "content": f"**Killmail URL:** {url}"
                            }
                            response = requests.post(DISCORD_WEBHOOK_URL, json=discord_payload)
                            
                            if response.status_code == 204:
                                print("Message sent to Discord successfully.")
                            else:
                                print(f"Failed to send message to Discord: {response.status_code} - {response.text}")
                        else:
                            print("Message does not contain required fields or does not meet the condition.")
                    except json.JSONDecodeError as e:
                        print(f"JSON decode error: {e} - Message: {message}")
                    except Exception as e:
                        print(f"An error occurred while processing the message: {e}")
        except websockets.ConnectionClosedError as e:
            print(f"WebSocket connection closed: {e}")
            print("Reconnecting in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            print(f"WebSocket connection error: {e}")
            print("Reconnecting in 5 seconds...")
            time.sleep(5)

if __name__ == "__main__":
    websocket_url = "wss://zkillboard.com/websocket/"
    subscription_payload = {
        "action": "sub",
        "channel": "killstream"
    }
    asyncio.run(receive_messages(websocket_url, subscription_payload))
