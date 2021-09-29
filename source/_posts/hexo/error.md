---
title : hexo错误集锦
tag : hexo
categories: 网站
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
> - 解决方案：重新检查配置一下域名解析。在项目根目录resource文件夹下添加CNAME文件。这样会自动生成时就上传到对应的master分支，GitHub就可以自动检测到域名解析并且自动配置了

![image-20210928200914375](https://tva1.sinaimg.cn/large/008i3skNgy1guwlmvksgwj61xs0u0te802.jpg)



## 3、文章置顶

【推荐】hexo-generator-index从 2.0.0 开始，已经支持文章置顶功能。你可以直接在文章的front-matter区域里添加sticky: 1属性来把这篇文章置顶。数值越大，置顶的优先级越大。





## 4、配置百度网站统计时报错

![image-20210929220215759](https://tva1.sinaimg.cn/large/008i3skNgy1guxuitbtnbj61pp0u0dni02.jpg)

> - 错误场景：  配置百度网站统计时报错
> - 错误原因： 
> - 解决方案：暂未解决





