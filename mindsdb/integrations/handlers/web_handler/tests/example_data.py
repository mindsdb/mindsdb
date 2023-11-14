PDF_CONTENT = (
    b"%PDF-1.7\n\n1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n\n2 0 obj\n<< /Type /Pages "
    b"/Kids [3 0 R] /Count 1 >>\nendobj\n\n3 0 obj\n<< /Type /Page /Parent 2 0 R /Contents 4 0 R "
    b">>\nendobj\n\n4 0 obj\n<< /Length 22 >>\nstream\nBT\n/Helvetica 12 Tf\n1 0 0 1 50 700 Tm\n("
    b"Hello, this is a test!) Tj\nET\nendstream\nendobj\n\nxref\n0 5\n0000000000 65535 "
    b"f\n0000000010 00000 n\n0000000077 00000 n\n0000000122 00000 n\n0000000203 00000 n\n0000000277 "
    b"00000 n\ntrailer\n<< /Size 5 /Root 1 0 R >>\nstartxref\n343\n%%EOF\n "
)

BROKEN_PDF_CONTENT = b"%PDF-1.4\n\nThis is not a valid PDF file content\n"

HTML_SAMPLE_1 = "<h1>Heading One</h1><h2>Heading Two</h2><ul><li>item1</li><li>item2</li><li>item3</li></ul>"

MARKDOWN_SAMPLE_1 = "# Heading One\n\n## Heading Two\n\n* item1\n* item2\n* item3\n\n"

HTML_SAMPLE_2 = '<h3>Heading</h3><p>text</p><a href="https://google.com">link</a><ul><ol>item1</ol></ul>'

MARKDOWN_SAMPLE_2 = "### Heading\n\ntext\n\n[link](https://google.com)\n\n\n\n"
