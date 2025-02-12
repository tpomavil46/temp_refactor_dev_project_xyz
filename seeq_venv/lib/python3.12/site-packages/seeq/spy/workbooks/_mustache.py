import json
import re

from seeq.spy import _common
from seeq.spy._errors import SPyDependencyNotFound


class MustachioedAnnotation:
    """
    Wraps a piece of HTML and exposes any Mustache variables in a way that is useful to the templating system. Also
    facilitates rendering of a template with Mustache variables replaced.
    """

    INVALID_MUSTACHE_CHARS = r'\.'
    INVALID_MUSTACHE_CHARS_REGEX = re.compile('[' + INVALID_MUSTACHE_CHARS + ']')

    _html: str
    _code_dict: dict

    def __init__(self, html, resolve_content_key_func=None):
        self._html = _common.fix_up_ckeditor_curly_brace_weirdness(html)

        if resolve_content_key_func is not None:
            self._html = MustachioedAnnotation._parameterize_seeq_content_ids(self._html, resolve_content_key_func)

        self._html = MustachioedAnnotation.fix_up_images(self._html)

        self._validate()

    @property
    def html(self):
        return self._html

    @property
    def code(self):
        return json.dumps(self._code_dict, indent=4).replace(': null', ': None')

    @property
    def code_dict(self):
        return self._code_dict

    @staticmethod
    def _parameterize_seeq_content_ids(html, resolve_content_key_func):
        return re.sub(r'data-seeq-content="([^"]+)"',
                      lambda m: f'data-seeq-content="{{{{{resolve_content_key_func(m.group(1))}}}}}"',
                      html)

    @staticmethod
    def fix_up_images(html):
        replacements = list()
        for img_tag_match in re.finditer(r'<img [^>]+>', html):
            # The reason for the \W? is that CKEditor puts in a zero-width space (Unicode character u200b) after the
            # first curly brace
            alt_attr_regex = r' alt="\{\W?\{([^}]+)}\W?}"'
            img_tag = img_tag_match.group(0)

            alt_attr_match = re.search(alt_attr_regex, img_tag)
            if alt_attr_match is None:
                continue

            var_name = alt_attr_match.group(1)
            new_img_tag = re.sub(r' src=\"/api/annotations/([^\"]+)\"', f' src="{{{{[Image] {var_name}}}}}"', img_tag)
            new_img_tag = re.sub(alt_attr_regex, f' alt="{{{{[AltText] {var_name}}}}}"', new_img_tag)
            replacements.append((img_tag, new_img_tag))

        for old, new in replacements:
            html = html.replace(old, new)
        return html

    # noinspection PyBroadException
    @staticmethod
    def invalid_mustache_characters():
        try:
            import chevron
        except ImportError:
            raise SPyDependencyNotFound(
                f'`Chevron` is required to use this feature. Please use '
                f'`pip install seeq-spy[templates]` to use this feature.')
        invalid_chars = str()
        for i in range(0, 65536):

            try:
                output = chevron.render('{{z' + chr(i) + 'z}}', {'z' + chr(i) + 'z': 'Worked!'})
                if output != 'Worked!':
                    invalid_chars += chr(i)
            except Exception:
                invalid_chars += chr(i)

        return invalid_chars

    @staticmethod
    def _sanitize_mustache_token(token):
        return MustachioedAnnotation.INVALID_MUSTACHE_CHARS_REGEX.sub('-', token)

    def _validate(self):
        matches = re.finditer(r'({{{?)([!#^/>=&]?)([^}]+)(}?}})', self.html)
        replacements = list()
        root_dict = dict()
        stack = list([root_dict])
        current_dict = root_dict
        for match in matches:
            prefix = match.group(1)
            qualifier = match.group(2)
            name = match.group(3)
            suffix = match.group(4)

            if name is None:
                continue

            sanitized_name = MustachioedAnnotation._sanitize_mustache_token(name)

            if qualifier is not None:
                replacements.append((match.group(0), prefix + qualifier + sanitized_name + suffix))
            else:
                replacements.append((match.group(0), prefix + sanitized_name + suffix))

            new_list = [dict()]
            if qualifier in ['#', '^']:
                stack.append(current_dict)
                if sanitized_name in current_dict:
                    current_dict = current_dict[sanitized_name][0]
                else:
                    current_dict[sanitized_name] = new_list
                    current_dict = new_list[0]
            elif qualifier == '/':
                current_dict = stack.pop()
            else:
                current_dict[sanitized_name] = None

        for _old, _new in replacements:
            self._html = self._html.replace(_old, _new)

        self._code_dict = root_dict

    def render(self, parameters: dict) -> str:
        try:
            import chevron
        except ImportError:
            raise SPyDependencyNotFound(
                f'`Chevron` is required to use this feature. Please use '
                f'`pip install seeq-spy[templates]` to use this feature.')

        data = dict()
        if parameters is not None:
            for key, value in parameters.items():
                data[key] = ('{{' + key + '}}') if value is None else value

        return chevron.render(self.html, data)
