/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const React = require('react');

const CompLibrary = require('../../core/CompLibrary.js');

const MarkdownBlock = CompLibrary.MarkdownBlock;
const Container = CompLibrary.Container;
const GridBlock = CompLibrary.GridBlock;

const siteConfig = require(process.cwd() + "/siteConfig.js");

function iconUrl(icon) {
  return siteConfig.baseUrl + "img/icon/" + icon;
}


class HomeSplash extends React.Component {
  render() {
    const {siteConfig, language = ''} = this.props;
    const {baseUrl, docsUrl} = siteConfig;
    const docsPart = `${docsUrl ? `${docsUrl}/` : ''}`;
    const langPart = `${language ? `${language}/` : ''}`;
    const docUrl = doc => `${baseUrl}${docsPart}${doc}`; // @TODO go back to ${baseUrl}${docsPart}${langPart}${doc} once docs are server properly (with en in the path)

    const SplashContainer = props => (
      <div className="homeContainer">
        <div className="homeSplashFade">
          <div className="wrapper homeWrapper">{props.children}</div>
        </div>
      </div>
    );

    const PromoSection = props => (
      <div className="section promoSection">
        <div className="promoRow">
          <div className="pluginRowBlock">{props.children}</div>
        </div>
      </div>
    );

    const ProjectTitle = () => (
      <h2 className="projectTitle">
        {siteConfig.title}
        <small>{siteConfig.tagline}</small>
      </h2>
    );

    return (
      <SplashContainer>
        <div className="inner">
          <ProjectTitle siteConfig={siteConfig} />
          <PromoSection>

          </PromoSection>
        </div>
      </SplashContainer>
    );
  }
}

class Index extends React.Component {
  render() {
    const {config: siteConfig, language = ''} = this.props;
    const {baseUrl} = siteConfig;

    const PromoSection = props => (
      <div className="section promoSection">
        <div className="promoRow">
          <div className="pluginRowBlock">{props.children}</div>
        </div>
      </div>
    );

    const Button = props => (
      <div className="pluginWrapper buttonWrapper">
        <a className="button" href={props.href} target={props.target}>
          {props.children}
        </a>
      </div>
    );

    const Block = props => (
      <Container
        padding={['bottom', 'top']}
        id={props.id}
        background={props.background}>
        <GridBlock
          align="center"
          contents={props.children}
          layout={props.layout}
        />
      </Container>
    );

    const Features = props => (
      <Block layout="fourColumn">
        {[
          {
            content:
              "MindsDB trains, tests and then selects the most accurate AI models to apply to your data. Giving you super accurate predictions and forecasting.",
            image: iconUrl("icon5.png"),
            imageAlign: "top",
            title: "Accurate Predictions"
          },
          {
            content:
              "Exceptionally easy to use, provide MindsDB access to your data and ask it what you want to forecast, and MindsDB takes it from there.",
            image: iconUrl("icon7.png"),
            imageAlign: "top",
            title: "Super easy to use"
          },
          {
            content:
              "Connect to your data where it lives. Run MindsDB on your local machine ensuring privacy, and that only you have access to your data.",
            image: iconUrl("privacy.png"),
            imageAlign: "top",
            title: "Privacy Centric"
          },
          {
            content:
              "Easily understand your predictions with MindsDB explainability. We believe Why, is just as important as what!",
            image: iconUrl("icon3.png"),
            imageAlign: "top",
            title: "Explainability – No Black Box"
          }
        ]}
      </Block>
    );

    const GetMindsdb = props => (
      <div
        className="productShowcaseSection paddingBottom downloadSection"
        style={{ textAlign: "center" }}
      >
        <h1>About Mindsdb</h1>
        <MarkdownBlock className="section-desc">
        We’ve known for sometime that the power of Deep Learning based Predictive Models will revolutionize the business world. But there is an obvious bottle-neck: the need for highly skilled data scientists to build these sophisticated models. Our mission is to address this bottleneck.
        Our product, MindsDB, integrates with existing databases, and through heuristics automatically builds and trains a set of Deep Learning based Predictive Models.
        </MarkdownBlock>
        <PromoSection>
          <Button
            href="https://github.com/mindsdb/mindsdb/releases"
          >
            Try Mindsdb
          </Button>
        </PromoSection>
      </div>
    );

    const Showcase = () => {
      if ((siteConfig.users || []).length === 0) {
        return null;
      }

      const showcase = siteConfig.users
        .filter(user => user.pinned)
        .map(user => (
          <a href={user.infoLink} key={user.infoLink}>
            <img src={user.image} alt={user.caption} title={user.caption} />
          </a>
        ));

      const pageUrl = page => baseUrl + (language ? `${language}/` : '') + page;

      return (
        <div className="productShowcaseSection paddingBottom">
          <h2>Who is Using This?</h2>
          <p>This project is used by all these people</p>
          <div className="logos">{showcase}</div>
          <div className="more-users">
            <a className="button" href={pageUrl('users.html')}>
              More {siteConfig.title} Users
            </a>
          </div>
        </div>
      );
    };

    return (
      <div>
        <HomeSplash siteConfig={siteConfig} language={language} />
        <div className="mainContainer">
          <Features />
          <Showcase />
          <GetMindsdb />
        </div>
      </div>
    );
  }
}

module.exports = Index;
