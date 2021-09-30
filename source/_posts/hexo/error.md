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



## 5、hexo部署至coding

1、[coding官网帮助文档，直接帮你搞定](https://help.coding.net/docs/pages/practice/hexo.html)

2、[博客一篇，有帮助吗](https://blog.csdn.net/qq_43194368/article/details/105128634?ops_request_misc=%257B%2522request%255Fid%2522%253A%2522163293481416780261939178%2522%252C%2522scm%2522%253A%252220140713.130102334..%2522%257D&request_id=163293481416780261939178&biz_id=0&utm_medium=distribute.pc_search_result.none-task-blog-2~all~sobaiduend~default-1-105128634.first_rank_v2_pc_rank_v29&utm_term=hexo+github+coding%E5%8F%8C%E7%BA%BF%E9%83%A8%E7%BD%B2&spm=1018.2226.3001.4187)

- **问题描述**

  部署之后出现问题：

![image-20210930090643321](https://i.loli.net/2021/09/30/TsyhcJefLXnDwRQ.png)

- **解决方法：**

检查域名和解析：发现之前的GitHub的ip是 185.199.109.153，现在的变了。把域名解析中添加了这个IP地址就正常进去了。是什么原因呢？以后还会出现吗

![image-20210930092734286](https://i.loli.net/2021/09/30/BlR48DhTXqrOgye.png)



## 6、在本地测试的时候，`hexo g `命令有Warn，No layout，导致项目无法启动

 换了一个电脑之后进行本地测试，之后本地的localhost:4000起不来。截图如下

![image-20210930092918849](https://i.loli.net/2021/09/30/ltkmDjS7w4gIa8C.png)

访问localhost的时候报错：

![image-20210930093104225](https://i.loli.net/2021/09/30/wrdaQfsYO4zLGS7.png)



## 7、网站的远程图片突然全部失效

> - 问题描述： 之前运行正常，晚上睡了一觉之后，图片就失效了，直接通过浏览器也是无法访问的。 但是通过GitHub中的MD显示，图片可以正常显示
> - 问题定位： 失效的图片都是家里Mac上传的图床服务器，在windows的是没问题的，尝试把图片复制到windows对应的服务器，以后把Mac的服务器也换一下。
> - 解决方法

![image-20210930093332641](https://i.loli.net/2021/09/30/K8BXHybCanNMjdO.png)

