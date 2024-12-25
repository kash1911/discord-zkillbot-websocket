import asyncio
import json
import aiohttp

DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1321044607076929577/foVcNHHMBNK2OL8YQYP17MmsP4nwcwVSRJWAo-KR1snfSYBjax5xA9wqQST40dPH5AFx"


async def receive_messages(websocket_url, subscription_payload):
    """
    Connects to the zKillboard WebSocket using aiohttp and listens for killstream data.
    Filters messages based on totalValue > 10,000,000,000 and sends to Discord if matched.
    Reconnects on connection errors.
    """
    while True:
        try:
            # Create a single aiohttp ClientSession to handle both WS and Discord POST
            async with aiohttp.ClientSession() as session:
                print(f"Connecting to WebSocket: {websocket_url}...")
                
                async with session.ws_connect(websocket_url) as ws:
                    print(f"Connected to WebSocket: {websocket_url}")

                    # Send the subscription payload
                    await ws.send_str(json.dumps(subscription_payload))
                    print(f"Sent subscription payload: {subscription_payload}")

                    # Continuously receive messages
                    while True:
                        msg = await ws.receive()
                        
                        # Check for normal text messages vs. error/close messages
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            raw_message = msg.data
                            print(f"Raw message received: {raw_message}")
                            try:
                                data = json.loads(raw_message)
                                print(f"Parsed JSON: {json.dumps(data, indent=4)}")
                                
                                # Extract required fields from the 'zkb' object
                                zkb_data = data.get("zkb", {})
                                total_value = zkb_data.get("totalValue")
                                url = zkb_data.get("url")

                                # Check if the required fields are present and meet the condition
                                if total_value is not None and url and total_value > 10_000_000_000:
                                    print(f"Filtered Message - URL: {url}")

                                    # Prepare the message for Discord
                                    discord_payload = {
                                        "content": f"**高价值击杀,sent by Fuxi killbot** {url}"
                                    }

                                    # Send the filtered message to Discord via the same session
                                    async with session.post(DISCORD_WEBHOOK_URL, json=discord_payload) as response:
                                        if response.status == 204:
                                            print("Message sent to Discord successfully.")
                                        else:
                                            error_text = await response.text()
                                            print(f"Failed to send message to Discord: {response.status} - {error_text}")
                                else:
                                    print("Message does not contain required fields or does not meet the condition.")
                            except json.JSONDecodeError as e:
                                print(f"JSON decode error: {e} - Message: {raw_message}")
                            except Exception as e:
                                print(f"An error occurred while processing the message: {e}")
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            # The connection closed or errored, break out to restart connection
                            print(f"WebSocket closed or errored: {msg.type} - {msg.data}")
                            break

        except aiohttp.ClientConnectorError as e:
            print(f"Connection error (ClientConnectorError): {e}")
            print("Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"WebSocket connection error: {e}")
            print("Reconnecting in 5 seconds...")
            await asyncio.sleep(5)


async def main():
    websocket_url = "wss://zkillboard.com/websocket/"
    subscription_payload = {
        "action": "sub",
        "channel": "killstream"
    }
    await receive_messages(websocket_url, subscription_payload)


if __name__ == "__main__":
    asyncio.run(main())
