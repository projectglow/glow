# Copyright 2019 The Glow Authors
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from docutils import nodes
from docutils.parsers import rst
from docutils.parsers.rst import directives
import os

class embedded_notebook(nodes.Special, nodes.Inline, nodes.PreBibliographic, nodes.FixedTextElement):
    pass

def visit_notebook_node(self, node):
    if 'html' in node.get('format', '').split():
        t = isinstance(node.parent, nodes.TextElement) and 'span' or 'div'
        if node['classes']:
            self.body.append(self.starttag(node, t, suffix=''))
        self.body.append(node.astext())
        if node['classes']:
            self.body.append('</%s>' % t)
        # Keep non-HTML raw text out of output:
    raise nodes.SkipNode

def depart_notebook_node(self, node):
    pass

class Notebook(rst.Directive):
    required_arguments = 2
    has_content = False
    option_spec = {
        'title': directives.unchanged,
        'slug': directives.unchanged,
        'width': directives.unchanged,
        'height': directives.unchanged,
        "force-link": directives.unchanged
        }

    def run(self):
        path_to_base = self.arguments[0]
        relative_notebook_path  = self.arguments[1]
        notebook_path_from_base = os.path.join(path_to_base, '_static/notebooks', relative_notebook_path)

        notebook_name = relative_notebook_path.split(".")[-2]
        opts = self.options
        title = opts['title'] if 'title' in opts else "Notebook"
        width = opts['width'] if 'width' in opts else "100%"
        height = opts['height'] if 'height' in opts else "1000px"
        node_id = nodes.make_id(notebook_name)

        id_hash = hash(notebook_path_from_base)

        raw_contents = """
<div class='embedded-notebook'>
    <a href="../additional-resources.html#running-databricks-notebooks">How to run a notebook</a>
    <a style='float:right' href="{url}">Get notebook link</a></p>
    <div class='embedded-notebook-container'>
        <div class='loading-spinner'></div>
        <iframe src="{url}" id='{id}' height="{h}" width="{w}" style="overflow-y:hidden;" scrolling="no"></iframe>
    </div>
</div>
""".format(id=id_hash, url=notebook_path_from_base, h=height, w=width)

        top_section = nodes.section(notebook_path_from_base, ids=[node_id])
        top_section += nodes.title(text=title)
        top_section += embedded_notebook('', raw_contents, format='html')
        nb = embedded_notebook('', raw_contents, format='html')

        return [ top_section ]


def setup(app):
    app.add_node(embedded_notebook, html=(visit_notebook_node, depart_notebook_node), latex=(lambda x, y: "", lambda x, y: ""))
    app.add_directive("notebook", Notebook)
