#!/usr/bin/env python3
# github_java_bug_collector.py
"""
抓取 GitHub 上 Java 项目（按 star 排序）自 cutoff_date 之后的 bug issues，
并尝试找到对应修复的 commit（或 PR 合并产生的 commit），导出 JSON 与 CSV，
并对 commit 做两个“可运行性”近似检查：
  A) 该 commit 下存在构建文件（pom.xml / build.gradle / build.gradle.kts / settings.gradle）
  B) 该 commit 在 GitHub 上的 checks/status 为成功（若可查询）

依赖: requests, python-dateutil, tqdm, pandas（可选）
    pip install requests python-dateutil tqdm pandas
用法示例:
    export GITHUB_TOKEN="ghp_xxx"
    python github_java_bug_collector.py --out-json out.json --out-csv out.csv

在 Jupyter 中也可直接运行（脚本使用 parse_known_args）。
"""
import os, sys, time, argparse, requests, json, csv, math
from datetime import datetime, date
from dateutil import parser as dateparser
from urllib.parse import quote_plus
from tqdm import tqdm

# -------------------- 配置（可通过命令行覆盖） --------------------
GITHUB_API = "https://api.github.com"
DEFAULT_CUTOFF = "2025-01-22"   # 保守使用 DeepSeek-V3 发布后日期（可修改）
MAX_REPOS = 200                 # 抓取 top N 仓库（按 stars）
REPOS_PER_PAGE = 50
MAX_ISSUES_PER_REPO = 50
MAX_COMMITS_PER_ISSUE = 5       # 对于每个 issue 最多采集多少 commit / PR
SLEEP_BETWEEN_REQUESTS = 0.25   # 防速率限制
# -----------------------------------------------------------------

def get_headers(token=None, extra_accept=None):
    h = {"Accept": "application/vnd.github+json"}
    if extra_accept:
        # e.g. for commit search: 'application/vnd.github.cloak-preview+json'
        h["Accept"] = extra_accept
    t = token or os.getenv("GITHUB_TOKEN")
    if t:
        h["Authorization"] = f"token {t}"
    return h

def api_get(url, params=None, token=None, extra_accept=None):
    headers = get_headers(token, extra_accept)
    r = requests.get(url, headers=headers, params=params, timeout=30)
    if r.status_code == 403:
        # 处理速率限制：尝试等待到 reset
        reset = r.headers.get("X-RateLimit-Reset")
        if reset:
            wait = int(reset) - int(time.time()) + 3
            wait = max(wait, 5)
            print(f"[rate-limit] waiting {wait}s...")
            time.sleep(wait)
            r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    time.sleep(SLEEP_BETWEEN_REQUESTS)
    return r.json(), r.headers

def search_repos_language_java(per_page=REPOS_PER_PAGE, max_repos=MAX_REPOS, token=None):
    repos = []
    page = 1
    while len(repos) < max_repos:
        params = {"q": "language:Java", "sort": "stars", "order": "desc", "per_page": per_page, "page": page}
        data, headers = api_get(f"{GITHUB_API}/search/repositories", params=params, token=token)
        items = data.get("items", [])
        if not items:
            break
        repos.extend(items)
        if len(items) < per_page:
            break
        page += 1
    return repos[:max_repos]

def search_issues_for_repo(repo_fullname, q_extra, per_page=100, max_issues=MAX_ISSUES_PER_REPO, token=None):
    items = []
    page = 1
    while len(items) < max_issues:
        q = f"repo:{repo_fullname} {q_extra}"
        params = {"q": q, "per_page": per_page, "page": page}
        data, headers = api_get(f"{GITHUB_API}/search/issues", params=params, token=token)
        hits = data.get("items", [])
        items.extend(hits)
        if len(hits) < per_page:
            break
        page += 1
        # GitHub search API 有 1000 条限制 per single query - we assume per repo it's small
    return items[:max_issues]

def find_commits_referencing_issue(repo_fullname, issue_number, token=None, max_results=MAX_COMMITS_PER_ISSUE):
    """
    使用 commit 搜索（需要特殊 Accept header）在 commit message 中搜索 issue number（例如 #123）
    也会尝试搜索 PRs that mention/close the issue (search issues with is:pr "closes #N")
    返回 commit sha 列表（可能为空）
    """
    commits = []
    owner_repo = repo_fullname
    # 1) 使用 commit search (cloak preview)
    q = f"repo:{owner_repo} \"#{issue_number}\""
    params = {"q": q, "per_page": max_results}
    try:
        data, _ = api_get(f"{GITHUB_API}/search/commits", params=params, token=token,
                         extra_accept="application/vnd.github.cloak-preview+json")
        for it in data.get("items", []):
            sha = it.get("sha")
            if sha:
                commits.append({"sha": sha, "message": it.get("commit", {}).get("message", ""), "url": it.get("url")})
    except Exception:
        # commit search may be disabled for token privileges; ignore errors
        pass

    # 2) 搜索 PRs that mention/close the issue
    if len(commits) < max_results:
        qpr = f"repo:{owner_repo} is:pr \"#{issue_number}\""
        try:
            data, _ = api_get(f"{GITHUB_API}/search/issues", params={"q": qpr, "per_page": max_results}, token=token)
            for pr in data.get("items", []):
                # 获取 PR 的合并 commit 或 merge_commit_sha
                pr_url = pr.get("pull_request", {}).get("url")
                if pr_url:
                    pr_data, _ = api_get(pr_url, token=token)
                    # 尽量找 merge_commit_sha 或 commits in PR
                    merge_sha = pr_data.get("merge_commit_sha")
                    if merge_sha:
                        commits.append({"sha": merge_sha, "message": pr_data.get("title", ""), "url": pr_url})
                    else:
                        # fallback: list commits in PR
                        commits_url = pr_data.get("commits_url")
                        if commits_url:
                            commit_list, _ = api_get(commits_url, token=token)
                            for c in commit_list[:max_results]:
                                commits.append({"sha": c.get("sha"), "message": c.get("commit", {}).get("message", ""), "url": commits_url})
        except Exception:
            pass

    # 去重并截断
    seen = set()
    out = []
    for c in commits:
        if not c.get("sha"):
            continue
        if c["sha"] in seen:
            continue
        seen.add(c["sha"])
        out.append(c)
        if len(out) >= max_results:
            break
    return out

def get_commit_details(repo_fullname, sha, token=None):
    # 获取 commit 详情（包含 files[] 和 patch 字段）
    url = f"{GITHUB_API}/repos/{repo_fullname}/commits/{sha}"
    data, _ = api_get(url, token=token)
    return data

def commit_has_build_file(repo_fullname, sha, token=None):
    # 检查常见构建文件是否存在于该 commit 的根目录
    candidates = ["pom.xml", "build.gradle", "build.gradle.kts", "settings.gradle", "pom.xml"]
    for fname in candidates:
        url = f"{GITHUB_API}/repos/{repo_fullname}/contents/{fname}"
        try:
            data, _ = api_get(url, params={"ref": sha}, token=token)
            if data and data.get("type") == "file":
                return True, fname
        except requests.HTTPError as e:
            # 404 等表示不存在，继续检查下一个
            continue
    return False, None

def commit_check_status_success(repo_fullname, sha, token=None):
    # 尝试获取 combined status 或 check-runs 状态
    try:
        data, _ = api_get(f"{GITHUB_API}/repos/{repo_fullname}/commits/{sha}/status", token=token)
        state = data.get("state")  # "success", "failure", "pending"
        if state == "success":
            return True, "status:success"
    except Exception:
        pass
    # check-runs
    try:
        data, _ = api_get(f"{GITHUB_API}/repos/{repo_fullname}/commits/{sha}/check-runs", token=token,
                         extra_accept="application/vnd.github+json")
        runs = data.get("check_runs", [])
        if runs:
            # 若至少有一个已完成并且 conclusion == success，则视为 success（这个判断可以更严格）
            for r in runs:
                if r.get("status") == "completed" and r.get("conclusion") == "success":
                    return True, f"check-run:{r.get('name')}"
    except Exception:
        pass
    return False, None

def normalize_issue_item(issue, repo_fullname):
    return {
        "issue_id": issue.get("id"),
        "issue_number": issue.get("number"),
        "issue_title": issue.get("title"),
        "issue_body": issue.get("body") or "",
        "issue_created_at": issue.get("created_at"),
        "issue_updated_at": issue.get("updated_at"),
        "issue_url": issue.get("html_url"),
        "repo_fullname": repo_fullname,
        "repo_url": issue.get("repository_url"),
        "user_login": (issue.get("user") or {}).get("login")
    }

def save_json(data, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[saved] {path}")

def save_csv(rows, path, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"[saved] {path}")

def main():
    ap = argparse.ArgumentParser(description="GitHub Java bug collector (issues -> commits/patches)")
    ap.add_argument("--out-json", default="issues_commits.json")
    ap.add_argument("--out-csv", default="issues_commits.csv")
    ap.add_argument("--token", default=None, help="GitHub token (or set GITHUB_TOKEN env var)")
    ap.add_argument("--cutoff", default=DEFAULT_CUTOFF,
                    help="只抓取 created_at >= cutoff 的 issues (YYYY-MM-DD)")
    ap.add_argument("--max-repos", type=int, default=100)
    ap.add_argument("--max-issues-per-repo", type=int, default=20)
    args, _unknown = ap.parse_known_args()   # use parse_known_args for Jupyter compatibility

    token = args.token or os.getenv("GITHUB_TOKEN")
    cutoff = dateparser.parse(args.cutoff).date()

    repos = search_repos_language_java(per_page=REPOS_PER_PAGE, max_repos=args.max_repos, token=token)
    print(f"[info] fetched {len(repos)} java repos (top by stars).")

    all_records = []
    for repo in tqdm(repos, desc="repos"):
        full = repo.get("full_name")
        # 搜索该 repo 中 label:bug 且 created >= cutoff
        q_extra = f'is:issue label:bug created:>={cutoff.isoformat()}'
        issues = search_issues_for_repo(full, q_extra, per_page=100, max_issues=args.max_issues_per_repo, token=token)
        if not issues:
            continue
        for iss in issues:
            if "pull_request" in iss:
                continue
            n = normalize_issue_item(iss, full)
            # 找引用该 issue 的 commits / PRs
            commits = find_commits_referencing_issue(full, n["issue_number"], token=token, max_results=MAX_COMMITS_PER_ISSUE)
            if not commits:
                # 尝试使用 issue timeline (高级)，或者跳过
                # 这里保守地记录 issue 但不附带 commit
                rec = {**n, **{
                    "commit_sha": None,
                    "commit_message": None,
                    "commit_date": None,
                    "patch": None,
                    "commit_has_build_file": False,
                    "commit_build_file_name": None,
                    "commit_check_success": False,
                    "commit_check_info": None
                }}
                all_records.append(rec)
                continue
            # 逐个 commit 取详细信息（到达第一个满足可运行条件的也会记录）
            recorded = False
            for c in commits:
                sha = c.get("sha")
                try:
                    cd = get_commit_details(full, sha, token=token)
                except Exception:
                    continue
                commit_msg = cd.get("commit", {}).get("message")
                commit_date = cd.get("commit", {}).get("committer", {}).get("date") or cd.get("commit", {}).get("author", {}).get("date")
                # 拼 patch（files 中的 patch 字段）
                patch_parts = []
                for f in cd.get("files", []):
                    p = f.get("patch")
                    if p:
                        header = f"--- a/{f.get('filename')}\n+++ b/{f.get('filename')}\n"
                        patch_parts.append(header + p)
                patch_text = "\n\n".join(patch_parts) if patch_parts else None
                # 检查构建文件
                has_build, build_name = commit_has_build_file(full, sha, token=token)
                # 检查 CI/checks 状态
                check_ok, check_info = commit_check_status_success(full, sha, token=token)

                rec = {**n, **{
                    "commit_sha": sha,
                    "commit_message": commit_msg,
                    "commit_date": commit_date,
                    "patch": patch_text,
                    "commit_has_build_file": has_build,
                    "commit_build_file_name": build_name,
                    "commit_check_success": check_ok,
                    "commit_check_info": check_info
                }}
                all_records.append(rec)
                # 若满足任一可运行近似（有 build file 且 check success），可以提前跳出 commits loop（可选）
                if has_build and check_ok:
                    recorded = True
                    break
            # end commits loop
        # end issues loop
    # end repos loop

    # 保存
    save_json(all_records, args.out_json)
    # CSV flatten（patch 可能很长，仍会写入）
    fieldnames = ["repo_fullname","repo_url","issue_id","issue_number","issue_title","issue_body","issue_created_at","issue_updated_at",
                  "issue_url","user_login","commit_sha","commit_message","commit_date","commit_has_build_file","commit_build_file_name",
                  "commit_check_success","commit_check_info","patch"]
    save_csv(all_records, args.out_csv, fieldnames)
    print("[done]")

if __name__ == "__main__":
    main()
