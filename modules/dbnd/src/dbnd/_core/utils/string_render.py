from typing import Union

from jinja2 import Template

import attr


@attr.s
class StringRenderer(object):
    template = attr.ib()  # type: Union[str,Template]
    jinja_mode = attr.ib(default=False)  # type: bool

    @classmethod
    def from_str(cls, template):
        jinja_mode = "{{" in template
        if jinja_mode:
            template = Template(template)
        return cls(template=template, jinja_mode=jinja_mode)

    def render_str(self, **ctx):
        if self.jinja_mode:
            return self.template.render(**ctx)

        return self.template.format(**ctx)
