import git
import os
import json
import sys
from datetime import datetime
from elasticsearch import Elasticsearch

# Elasticsearch Configuration
ES_HOST = "http://localhost:9200"
ES_INDEX = "git-metadata"

def index_git_metadata(repo_path):
    """Extract and index Git metadata in Elasticsearch."""
    if not os.path.isdir(repo_path) or not os.path.isdir(os.path.join(repo_path, ".git")):
        print(f"Error: '{repo_path}' is not a valid Git repository.")
        sys.exit(1)

    # Initialize Elasticsearch client
    es = Elasticsearch(ES_HOST)

    # Initialize GitPython repo object
    repo = git.Repo(repo_path)

    for file_path in repo.git.ls_files().split("\n"):
        try:
            last_commit = next(repo.iter_commits(paths=file_path, max_count=1))
            last_modified = datetime.utcfromtimestamp(last_commit.committed_date).isoformat()

            # Get file contents at latest commit
            file_content = repo.git.show(f"{last_commit.hexsha}:{file_path}")

            # Get commit history
            commit_history = [
                {
                    "commit": commit.hexsha,
                    "author": commit.author.name,
                    "date": datetime.utcfromtimestamp(commit.committed_date).isoformat(),
                    "message": commit.message.strip()
                }
                for commit in repo.iter_commits(paths=file_path)
            ]

            # Get Git blame data (who last changed each line)
            blame_data = []
            for commit, lines in repo.blame("HEAD", file_path):
                for line in lines:
                    blame_data.append({
                        "commit": commit.hexsha,
                        "author": commit.author.name,
                        "date": datetime.utcfromtimestamp(commit.committed_date).isoformat(),
                        "line": line.strip()
                    })

            # Elasticsearch document structure
            doc = {
                "file_path": file_path,
                "last_modified": last_modified,
                "last_commit": last_commit.hexsha,
                "last_commit_message": last_commit.message.strip(),
                "last_author": last_commit.author.name,
                "commit_history": commit_history,
                "blame": blame_data,
                "file_content": file_content
            }

            # Index document in Elasticsearch
            es.index(index=ES_INDEX, id=file_path, body=doc)
            print(f"Indexed: {file_path}")

        except StopIteration:
            print(f"Skipping {file_path}, no commit history.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python git_indexer.py <repo_path>")
        sys.exit(1)

    repo_path = sys.argv[1]
    index_git_metadata(repo_path)
