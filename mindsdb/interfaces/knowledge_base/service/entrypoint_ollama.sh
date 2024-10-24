#!/bin/bash

# Start Ollama in the background.
/bin/ollama serve &
# Record Process ID.
pid=$!

#echo "FROM mistral:7b" > mistral.modelfile

# Pause for Ollama to start.
sleep 5

echo "🔴 Running our models..."
#ollama pull mistral:7b
ollama pull nomic-embed-text
#ollama create mistral-2 -f mistral.modelfile
echo "🟢 Done!"

# Wait for Ollama process to finish.
wait $pid
