# CS213_project

## 前言

本仓库来源于数据库原理的 project，目的是设计实验以尝试回答下面的问题：

- 与文件中的数据操作相比，DBMS有哪些独特优势？
- 哪个 DBMS 更好？是 PostgreSQL 还是 openGauss，以什么标准来评判？



本仓库中的java代码还可用于测试各种sql命令的延迟。

为了使用这个仓库内的代码，您需要在Docker中安装PostgreSQL和openGauss，其中容器名需要设为`postgresql` 和 `opengauss`。PostgreSQL绑定到宿主机的5432端口，openGauss绑定到宿主机的5433端口。

## 目录说明

- results文件夹：包含单次测试结果
- src文件夹：代码



其中src文件夹内含三个文件夹：

- java：解决第一个问题：与文件中的数据操作相比，DBMS有哪些独特优势？
- ps1：运行 pgbench
- python：分析数据

