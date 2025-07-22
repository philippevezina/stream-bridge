#!/bin/bash

# Parse the JSON input to extract file path
input=$(cat)
file_path=$(echo "$input" | jq -r '.tool_input.file_path // .tool_response.filePath // empty')

# Check if the modified file is a Go file
if [[ "$file_path" == *.go ]] && [[ -n "$file_path" ]]; then
    echo "Formatting Go file: $file_path"
    go fmt "$file_path"
fi