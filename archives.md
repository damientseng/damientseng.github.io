---
layout: page
---



<article class="post">
<header class="post-header">
<h2 class="post-title">Archive</h2>
</header>
<div class="Table">
{% for post in site.posts %}
<div class="Row">
  <div clas="Cell">{{post.date | date_to_string}}</div>
  <div class="Cell"><a href="{{ post.url }}">{{ post.title }}</a></div>
</div>
{% endfor %}
</div>
</article>
