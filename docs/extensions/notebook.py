from docutils import nodes
from docutils.parsers import rst
from docutils.parsers.rst import directives

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
    required_arguments = 1
    has_content = False
    option_spec = {
        'title': directives.unchanged,
        'slug': directives.unchanged,
        'width': directives.unchanged,
        'height': directives.unchanged,
        "force-link": directives.unchanged
        }

    def run(self):
        raw_file_path = self.arguments[0]

        raw_file_name = raw_file_path.split(".")[-2]
        opts = self.options
        title = opts['title'] if 'title' in opts else "Notebook"
        width = opts['width'] if 'width' in opts else "100%"
        height = opts['height'] if 'height' in opts else "1000px"
        node_id = nodes.make_id(raw_file_name)

        id_hash = hash(raw_file_path)

        raw_contents = """
<div class='embedded-notebook'>
    <a href="../additional-resources.html#running-databricks-notebooks">How to run a notebook</a>
    <a style='float:right' href="{url}">Get notebook link</a></p>
    <div class='embedded-notebook-container'>
        <div class='loading-spinner'></div>
        <iframe src="{url}" id='{id}' height="{h}" width="{w}" style="overflow-y:hidden;" scrolling="no"></iframe>
    </div>
</div>
""".format(id=id_hash, url=raw_file_path, h=height, w=width)

        top_section = nodes.section(raw_file_path, ids=[node_id])
        top_section += nodes.title(text=title)
        top_section += embedded_notebook('', raw_contents, format='html')
        nb = embedded_notebook('', raw_contents, format='html')

        return [ top_section ]


def setup(app):
    app.add_node(embedded_notebook, html=(visit_notebook_node, depart_notebook_node), latex=(lambda x, y: "", lambda x, y: ""))
    app.add_directive("notebook", Notebook)
