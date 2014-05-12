#goose
goose是使用golang开发检索框架,目标是打造一个简单方便使用的小型检索系统.
期望解决百万量级的doc数的.

goose这个名称的由来:
由于是使用golang开发的search engine,一开始想到就是用gose这个单词,但是其没有什么
含义,突然发现往中间加多一个字母o,就形成了一个有意义的单词 ***goose*** ,另外想到
很多牛X的项目(或语言)都以动物名字来命名,所以觉得 ***goose*** 这个名字挺好.

##如何使用?
goose是一个检索框架实现,由几个基础模块组成:

* `database`负责底层的静态索引,动态索引,ID管理,Value管理,Data管理.
* `config`模块简单实现的用于读取配置的模块.
* `log`是日志模块的封装.
* `utils`包含了goose的基础类型定义以及其它一些小工具类.
* `GooseBuild.go`和`Indexer.go`是主要的建库流程实现.
* `GooseSearch.go`和`Searcher.go`是主要的检索流程实现.
* `IStrategy.go`是检索策略需要关注以及实现的细节.

###策略实现
使用goose开发(小型的)检索系统,需要实现IStrategy.go所定义的策略.
[goose-demo](https://github.com/getwe/goose-demo)是一个实现demo,它演示了如果使用goose进行二次开发.
###零编码
为了更加方便,快速搭建一个检索系统,[cse](https://github.com/getwe/cse)是正在开发中的一个项目,它基于goose实现了一个零编码的小型通用检索系统,只要修改配置,按照要求的格式准备好输入数据,就可以直接建库后提供检索服务.  
目前,还在开发中...

##goose的设计
这里介绍goose的内部设计  
todo ..