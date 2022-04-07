# mydb

my database practice (B+tree, mmap, transaction)

代码阅读顺序

page -> node -> freelist -> cursor -> tx -> bucket -> db

关于mmap相关代码，可以去bolt那边看bolt_*相关的文件，因为都是平台相关代码，所以就不拷过来，

这里基本把核心的都看了，注释也翻译完了

## 参考

https://github.com/boltdb/bolt