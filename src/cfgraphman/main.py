import calendar
import json
from pathlib import Path

import click
import pygtrie
from pygit2 import Repository, Signature, discover_repository, init_repository
from pygit2.enums import FileMode

OLD_TIME = calendar.timegm((2018, 1, 1, 0, 0, 0, 0, 1, 0))


def init_repo(path):
    path = path.as_posix()
    repo = discover_repository(path)
    if repo is not None:
        return Repository(repo)
    repo = init_repository(path, True)
    ref = "HEAD"
    author = Signature("Author", "author@conda-forge.org")
    committer = Signature("Committer", "committer@conda-forge.org")
    parents = []
    tree = repo.TreeBuilder().write()
    repo.create_commit(ref, author, committer, "Initial commit", tree, parents)
    return repo


def load_artifact(path):
    info = json.load(path.open())
    return info


def info_to_artifact_id(info):
    idx = info["index"]
    return f"{idx['subdir']}/{idx['name']}-{idx['version']}-{idx['build']}"


def add_artifact_to_repo(repo, info):
    artifact_id = info_to_artifact_id(info)
    try:
        timestamp = info["index"]["timestamp"] / 1000.0
    except KeyError:
        timestamp = OLD_TIME

    ref = repo.head.name
    author = Signature("Author", "author@conda-forge.org", int(timestamp))
    committer = Signature("Committer", "committer@conda-forge.org")
    current_commit = repo.head.target
    parents = [current_commit]
    trie = pygtrie.StringTrie.fromkeys(info["files"], value=True)
    current_tree = repo.get(current_commit).tree
    tree_map = {tuple(): current_tree}

    def cb(path_conv, path, children, is_file=False):
        if path == tuple():
            self_entry = tree_map[path]
        else:
            parent_tree = tree_map.get(path[:-1])
            try:
                self_entry = None if parent_tree is None else parent_tree[path[-1]]
            except KeyError:
                self_entry = None
        if is_file:
            if self_entry is None:
                blob = repo.create_blob(f"{artifact_id}\n")
            else:
                if f"{artifact_id}\n".encode() in (old_data := self_entry.data):
                    blob = None
                else:
                    blob = repo.create_blob(old_data + f"{artifact_id}\n".encode())
            return (path[-1], blob, FileMode.BLOB)
        tree_map[path] = self_entry
        tb = repo.TreeBuilder() if self_entry is None else repo.TreeBuilder(self_entry)
        for child in children:
            if child[1] is not None:
                tb.insert(*child)
        tree = tb.write()
        tree_map[path] = tree
        if path:
            return (path[-1], tree, FileMode.TREE)
        else:
            return tree

    root = trie.traverse(cb)
    repo.create_commit(ref, author, committer, f"Adding {artifact_id}", root, parents)


@click.command()
@click.option("-r", "--repo", type=Path, default=Path("cfgraph.git"))
@click.option("-f", "--file", type=Path)
@click.argument("artifacts", type=Path, nargs=-1)
def cli(repo, file, artifacts):
    if file is not None:
        artifact_file = file.open()
        artifacts = (Path(path[:-1]) for path in artifact_file)
    repo = init_repo(repo)
    for artifact in artifacts:
        info = load_artifact(artifact)
        add_artifact_to_repo(repo, info)


if __name__ == "__main__":
    cli()
