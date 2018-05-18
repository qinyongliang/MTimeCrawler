# 时光网爬虫
爬取电影之间的关系并放在图形数据库arangodb中。


# 导出数据命令
```bash
arangoexport --type jsonl --collection leaguer --collection leaguer_edge --collection movie --collection person --collection personate_edge --collection relation_edge --server.database mtime --server.password 123456 --output-directory "dump"
```
# 导入数据命令
```bash
ls |awk -F . '{print $1}'|xargs -i arangoimp --threads 4 --file "{}.jsonl" --type jsonl --collection "{}" --server.database mtime --server.password 123456
```
