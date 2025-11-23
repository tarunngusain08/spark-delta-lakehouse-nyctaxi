#!/bin/zsh

# Check if a commit message is provided
if [ $# -eq 0 ]; then
    echo "Please provide a base commit message"
    exit 1
fi

BASE_MESSAGE="$1"

# Change to the root of the git repository
cd "$(git rev-parse --show-toplevel)"

# Function to check file size (GitHub limit is 100MB)
check_file_size() {
    local file="$1"
    if [ -f "$file" ]; then
        local size=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null || echo "0")
        local size_mb=$((size / 1024 / 1024))
        if [ $size_mb -gt 100 ]; then
            echo "WARNING: $file is ${size_mb}MB (exceeds GitHub's 100MB limit) - skipping"
            return 1
        fi
    fi
    return 0
}

# Get list of changed files, excluding those in .gitignore
CHANGED_FILES=($(git ls-files -m -o --exclude-standard))

# Commit each file separately
for file in "${CHANGED_FILES[@]}"; do
    # Skip if file is ignored
    if git check-ignore -q "$file"; then
        echo "Skipping ignored file: $file"
        continue
    fi
    
    # Check file size before committing
    if ! check_file_size "$file"; then
        continue
    fi
    
    # Stage only this file
    git add "$file"
    
    # Commit with a specific message
    git commit -m "$BASE_MESSAGE: ${file##*/}"
    
    echo "Committed changes for $file"
done

# Push all commits
git push origin HEAD

echo "All changes committed and pushed successfully!"
