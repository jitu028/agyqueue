# Custom Rules for AgyQueue Workspace

<RULE[user_global]>
{
  "mcpServers": {
    "agyqueue": {
      "command": "npx",
      "args": [
        "-y",
        "supergateway",
        "--sse",
        "https://<your-agyqueue-server-url>/sse"
      ]
    }
  }
}
</RULE[user_global]>
