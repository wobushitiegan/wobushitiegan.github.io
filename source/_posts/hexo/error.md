title : hexo错误集锦

---

> butterfly 主题的官方文档，有问题先去这查。https://butterfly.js.org/

## 1、err: YAMLException: end of the stream or a document separator is expected at line 35, column 1:

![image-20210928191727973](https://tva1.sinaimg.cn/large/008i3skNgy1guwk51d2ygj61jc0jo44602.jpg)

> - 错误场景：  在执行npx hexo g 时报错，目前还未找到解决方案。
> - 错误原因： YAML语法问题。 title设置的问题
> - 解决方案：检查title及 tag等配置，截图如下

![image-20210928200435572](https://tva1.sinaimg.cn/large/008i3skNgy1guwli180ytj60ro072glp02.jpg)

## 2、404There isn't a GitHub Pages site here.

![image-20210928200612329](https://tva1.sinaimg.cn/large/008i3skNgy1guwljphyo4j62r80scn0d02.jpg)

> - 错误场景：  hexo d,  部署了之后，网站就进不去了
> - 错误原因： 检查github的page，看域名解析是否还在，我这边是因为没有了，重新配置上就好了。
> - 解决方案：重新配置一下域名解析。（ps,我得找到根本原因，应该可以自动配置）

![image-20210928200914375](https://tva1.sinaimg.cn/large/008i3skNgy1guwlmvksgwj61xs0u0te802.jpg)



## 3、文章置顶

【推荐】hexo-generator-index从 2.0.0 开始，已经支持文章置顶功能。你可以直接在文章的front-matter区域里添加sticky: 1属性来把这篇文章置顶。数值越大，置顶的优先级越大。











bottom:

​    \- class_name: personal-wechat

​      id_name: personal-wechat

​      name: 个人微信

​      icon: iconfont icon-weixin

​      order:

​      html: <img width=230 height=230 src="https://tva1.sinaimg.cn/large/008i3skNgy1guwqa94ridj60rs0rq42702.jpg" alt="personal-wechat" />