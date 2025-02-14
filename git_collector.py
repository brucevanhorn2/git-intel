import git
import os
import json
import sys
from datetime import datetime
from elasticsearch import Elasticsearch

# Elasticsearch Configuration
ES_HOST = "http://localhost:9200"
ES_INDEX = "git-metadata"

def index_git_metadata(repo_path, output_json=False):
    """Extract Git metadata and either index in Elasticsearch or print as JSON."""
    if not os.path.isdir(repo_path) or not os.path.isdir(os.path.join(repo_path, ".git")):
        print(f"Error: '{repo_path}' is not a valid Git repository.")
        sys.exit(1)

    # Initialize GitPython repo object
    repo = git.Repo(repo_path)

    # Initialize Elasticsearch client only if not outputting JSON
    es = Elasticsearch(ES_HOST) if not output_json else None

    indexed_data = []

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

            # Construct the document
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

            if output_json:
                indexed_data.append(doc)
            else:
                es.index(index=ES_INDEX, id=file_path, body=doc)
                print(f"Indexed: {file_path}")

        except StopIteration:
            print(f"Skipping {file_path}, no commit history.")

    if output_json:
        print(json.dumps(indexed_data, indent=4))

if __name__ == "__main__":
    # if len(sys.argv) < 2 or len(sys.argv) > 3:
    #     print("Usage: python git_indexer.py <repo_path> [--output json]")
    #     sys.exit(1)

    repo_path = sys.argv[1]
    output_json = "--output" in sys.argv and sys.argv[sys.argv.index("--output") + 1] == "json"

    index_git_metadata(repo_path, output_json)
