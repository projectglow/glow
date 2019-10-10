from docutils import nodes
from docutils.parsers import rst
from docutils.parsers.rst import directives
import urllib
import os
import logging

NOTEBOOK_FILES = []
NOTEBOOK_ROOT = ''

def get_nb_size(nb_name):
    raw_path = os.path.abspath("_static/notebooks") + "/" + nb_name
    return os.path.getsize(raw_path)

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

    def validate(self):
        raw_file_path = self.arguments[0]
        url_encoded_file_path = urllib.parse.quote(raw_file_path, "/+")



        try:
            assert raw_file_path in NOTEBOOK_FILES
        except AssertionError:
            logging.error("""
{} is not a valid notebook path.
Make sure that the notebook can be found in the source/_static/notebooks/ folder.
""".format(raw_file_path))
            exit(1)

        try:
            assert raw_file_path == raw_file_path.lower()
            assert " " not in raw_file_path
            assert "_" not in raw_file_path
        except AssertionError:
            logging.error("""
{} is not a valid notebook name.
Make sure that all characters are lowercase.
Make sure that there are no spaces or underscores(_).""".format(raw_file_path))
            exit(1)

        try:
            assert url_encoded_file_path == raw_file_path
        except AssertionError:
            logging.error("URL quoted path does not equal original path")
            exit(1)

        if get_nb_size(raw_file_path) > 1000000:
            logging.info("""
{} is > 1MB in size. It is {}MB in size.
Try to make this smaller.
""".format(raw_file_path, get_nb_size(raw_file_path)/1000000))


    def run(self):
        raw_file_path = self.arguments[0]
        self.validate()

        raw_file_name = raw_file_path.split(".")[0]
        opts = self.options
        title = opts['title'] if 'title' in opts else "Notebook"
        width = opts['width'] if 'width' in opts else "100%"
        height = opts['height'] if 'height' in opts else "1000px"
        force_inline = True if 'force-link' in opts else False
        node_id = nodes.make_id(raw_file_name)

        url_encoded_file_path = urllib.parse.quote(raw_file_path, "/+")
        notebook_url = os.path.join(NOTEBOOK_ROOT, url_encoded_file_path)
        nb_size = get_nb_size(raw_file_path)
        id_hash = hash(raw_file_path)

        if nb_size < 1000000 and not force_inline:
            raw_contents = """
<div class='embedded-notebook'>
    <a style='float:right' href="{{ pathto('_static/notebooks/{url}', 1) }}">Get notebook link</a></p>
    <div class='embedded-notebook-container'>
        <div class='loading-spinner'></div>
        <iframe src="{url}', 1) }}" id='{id}' height="{h}" width="{w}" style="overflow-y:hidden;" scrolling="no"></iframe>
    </div>
</div>
""".format(id=id_hash, url=url_encoded_file_path, h=height, w=width)
        else:
            raw_contents = """
<div class='embedded-notebook'>
    <a style='float:right' href="{url}">Get notebook link</a>
    </p>
</div>
<p></p>
<p></p>
<p>This notebook is too large to display inline. <a href="{url}">Get notebook link</a>.</p>
""".format(id=id_hash, url=notebook_url, h=height, w=width)

        top_section = nodes.section(raw_file_path, ids=[node_id])
        top_section += nodes.title(text=title)
        top_section += embedded_notebook('', raw_contents, format='html')
        nb = embedded_notebook('', raw_contents, format='html')

        return [ top_section ]


def setup(app):
    global NOTEBOOK_FILES
    global NOTEBOOK_ROOT

    # could make this a recursive search through sub-directories
    path = os.path.abspath("_static/notebooks")
    NOTEBOOK_FILES = [os.path.join(dp, f) for dp, dn, fn in os.walk(path) for f in fn] # get all
    NOTEBOOK_FILES = [x[len(path) + 1:] for x in NOTEBOOK_FILES] # remove beginning string
    print("Notebooks at " + path + ": " + ", ".join(NOTEBOOK_FILES))

    NOTEBOOK_ROOT = os.path.join(app.config.html_baseurl, '_static/notebooks')
    print("Notebook root at " + NOTEBOOK_ROOT)

    app.add_node(embedded_notebook, html=(visit_notebook_node, depart_notebook_node), latex=(lambda x, y: "", lambda x, y: ""))
    app.add_directive("notebook", Notebook)
