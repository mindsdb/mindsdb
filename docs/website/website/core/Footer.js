/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const React = require('react');

class Footer extends React.Component {
  docUrl(doc, language) {
    const baseUrl = this.props.config.baseUrl;
    const docsUrl = this.props.config.docsUrl;
    const docsPart = `${docsUrl ? `${docsUrl}/` : ''}`;
    const langPart = `${language ? `${language}/` : ''}`;
    return `${baseUrl}${docsPart}${langPart}${doc}`;
  }

  pageUrl(doc, language) {
    const baseUrl = this.props.config.baseUrl;
    return baseUrl + (language ? `${language}/` : '') + doc;
  }

  render() {
    const currentYear = new Date().getFullYear();
    return (
      <footer className="nav-footer" id="footer">
        <section className="sitemap">
          <a href={this.props.config.baseUrl} className="nav-home">
            {this.props.config.footerIcon && (
              <img
                src={this.props.config.baseUrl + this.props.config.footerIcon}
                alt={this.props.config.title}
                width="66"
                height="58"
              />
            )}
          </a>
          <div>
            <h5>Docs</h5>
            <a href={this.docUrl('Installing', this.props.language)}>
              Installing MindsDB
            </a>
            <a href={this.docUrl('BasicExample', this.props.language)}>
              Learning from Examples
            </a>
            <a href={this.docUrl('FAQ', this.props.language)}>
              Frequently Asked Questions
            </a>
          </div>
          <div>
            <h5>Community</h5>
            <a
              href="https://medium.com/mindsdb"
              target="_blank"
              rel="noreferrer noopener">
              Medium
            </a>
            <a href="https://www.facebook.com/MindsDB/"
              target="_blank">
              Facebook
            </a>
            <a
              href="https://twitter.com/mindsdb?lang=en"
              target="_blank"
              rel="noreferrer noopener">
              Twitter
            </a>
            <a
              href="https://www.youtube.com/channel/UC5_wBOLCWath6q1iTgPPD5A"
              target="_blank">
              Youtube
            </a>
          </div>
          <div>
            <h5>More</h5>
            <a href={`${this.props.config.baseUrl}blog`}>Blog</a>
            <a href="https://github.com/mindsdb/mindsdb">GitHub</a>
            <a
              className="github-button"
              href={this.props.config.repoUrl}
              data-icon="octicon-star"
              data-count-href="/mindsdb/mindsdb/stargazers"
              data-show-count="true"
              data-count-aria-label="# stargazers on GitHub"
              aria-label="Star this project on GitHub">
              Star
            </a>
          </div>
        </section>

        <section className="copyright">
          Copyright &copy; {currentYear} <a href="http://mindsdb.com/">Mindsdb</a>
        </section>
      </footer>
    );
  }
}

module.exports = Footer;
