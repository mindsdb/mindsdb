import { 
  Sparkles, 
  TrendingUp, 
  MessageSquare, 
  BarChart3, 
  Zap, 
  Brain,
  ArrowRight,
  CheckCircle2,
  ChevronRight,
  Briefcase,
  Lightbulb,
  X,
  Target,
  TrendingDown
} from 'lucide-react';
import { useEffect } from 'react';
import Navbar from '../components/Navbar'; 
import Footer from '../components/Footer';

const Landing = () => {
  useEffect(() => {
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }, []);

  const features = [
    {
      icon: <MessageSquare className="h-6 w-6" />,
      title: "Talk Like a Human",
      description: "Ask anything in plain English. No SQL. No technical jargon. Just natural conversation with your data."
    },
    {
      icon: <Brain className="h-6 w-6" />,
      title: "Two Modes, One Experience",
      description: "Standard semantic search for quick insights, or advanced agent analytics for deep statistical breakdowns"
    },
    {
      icon: <BarChart3 className="h-6 w-6" />,
      title: "Beautiful Visualizations",
      description: "Stunning charts, heatmaps, and distributions generated instantly — no setup required"
    },
    {
      icon: <Lightbulb className="h-6 w-6" />,
      title: "AI-Powered Insights",
      description: "InsightInterpreter cuts through the noise and tells you what actually matters — and what to do about it"
    },
    {
      icon: <Zap className="h-6 w-6" />,
      title: "Intelligent Semantic Search",
      description: "MindsDB-powered embeddings find the most relevant reviews, even with complex or fuzzy queries"
    },
    {
      icon: <Briefcase className="h-6 w-6" />,
      title: "Management-Ready",
      description: "Insights you can act on immediately — no data science degree needed"
    }
  ];

  const agentQueries = [
    {
      query: "Out of all reviews mentioning slow boarding, what percentage of passengers rated value for money above 4?",
      insight: "Fast threshold KPI to gauge price fairness after slow boarding"
    },
    {
      query: "For passengers complaining about check-in delays, show seat type distribution among those who rated ground service above 3.",
      insight: "See how check-in issues vary by cabin class when ground service is decent"
    },
    {
      query: "Among reviews mentioning crowded flights, how many Economy Class passengers were Solo Leisure travelers compared to Business travelers?",
      insight: "Compare traveler mix across classes to target interventions"
    },
    {
      query: "Users who complained about baggage claim delays, what percentage of those who rated ground service above 4 rated overall experience below 5?",
      insight: "Spot disconnects between a strong touchpoint and weak overall satisfaction"
    },
    {
      query: "Among users who mentioned poor inflight meals, what percent of those who rated food and beverages above 4 also recommended the airline?",
      insight: "Check if high meal scores align with recommendation intent"
    },
    {
      query: "Show me reviews of users who had the best flight experience",
      insight: "Pure semantic search to surface your happiest flyers"
    }
  ];

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 via-indigo-50 to-purple-50">
      <style>{`
        @keyframes fadeInUp {
          from {
            opacity: 0;
            transform: translateY(30px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }

        @keyframes sparkle {
          0%, 100% {
            transform: rotate(0deg) scale(1);
            opacity: 1;
          }
          50% {
            transform: rotate(180deg) scale(1.1);
            opacity: 0.8;
          }
        }

        @keyframes float {
          0%, 100% {
            transform: translateY(0px);
          }
          50% {
            transform: translateY(-5px);
          }
        }

        @keyframes pulse-glow {
          0%, 100% {
            box-shadow: 0 0 20px rgba(79, 70, 229, 0.3);
          }
          50% {
            box-shadow: 0 0 40px rgba(79, 70, 229, 0.5);
          }
        }

        .animate-fade-in-up {
          animation: fadeInUp 0.8s ease-out forwards;
        }

        .animate-sparkle {
          animation: sparkle 3s ease-in-out infinite;
        }

        .animate-float {
          animation: float 3s ease-in-out infinite;
        }

        .animate-pulse-glow {
          animation: pulse-glow 2s ease-in-out infinite;
        }

        .delay-100 { animation-delay: 0.1s; }
        .delay-200 { animation-delay: 0.2s; }
        .delay-300 { animation-delay: 0.3s; }
        .delay-400 { animation-delay: 0.4s; }
        .delay-500 { animation-delay: 0.5s; }
        .delay-600 { animation-delay: 0.6s; }
      `}</style>

      <Navbar />
      
      {/* Hero Section */}
      <section className="pt-32 pb-20 px-4 sm:px-6 lg:px-8">
        <div className="max-w-7xl mx-auto">
          <div className="text-center space-y-8">
            <div className="inline-flex items-center space-x-2 bg-blue-50 border border-blue-200 rounded-full px-4 py-2 text-blue-700 text-sm font-medium animate-fade-in-up opacity-0">
              <Sparkles className="h-4 w-4 animate-sparkle" />
              <span>MindsDB Hacktoberfest 2025 Project</span>
            </div>

            <h1 className="text-5xl sm:text-6xl lg:text-5xl font-bold text-gray-900 leading-tight animate-fade-in-up delay-100 opacity-0">
              From Messy Reviews to
              <br />
              <span className="bg-gradient-to-r from-blue-600 via-indigo-600 to-purple-600 bg-clip-text text-transparent">
                Flight-Ready Intelligence
              </span>
            </h1>

            <p className="text-xl text-gray-600 max-w-3xl mx-auto leading-relaxed animate-fade-in-up delay-200 opacity-0">
              Ask questions in plain English. Get instant analytics, beautiful charts, and AI insights 
              that tell you exactly what to fix. Powered by MindsDB's intelligent search and dual-agent architecture.
            </p>

            <div className="flex flex-col sm:flex-row items-center justify-center gap-4 pt-4 animate-fade-in-up delay-300 opacity-0">
              <a
                href="/home"
                className="group inline-flex items-center space-x-2 bg-gradient-to-r from-blue-600 to-indigo-600 text-white px-8 py-4 rounded-xl font-semibold shadow-lg hover:shadow-xl transform hover:scale-105 transition-all duration-300 animate-pulse-glow"
              >
                <span>Unlock Insights</span>
                <ArrowRight className="h-5 w-5 group-hover:translate-x-1 transition-transform" />
              </a>
              <a
                href="/about"
                className="inline-flex items-center space-x-2 bg-white text-gray-700 px-8 py-4 rounded-xl font-semibold border-2 border-gray-200 hover:border-blue-300 hover:bg-blue-50 transition-all duration-300"
              >
                <span>How It Works</span>
                <ChevronRight className="h-5 w-5" />
              </a>
            </div>

            <div className="flex flex-wrap items-center justify-center gap-8 pt-8 text-sm text-gray-500 animate-fade-in-up delay-400 opacity-0">
              <div className="flex items-center space-x-2 animate-float">
                <CheckCircle2 className="h-5 w-5 text-green-500" />
                <span>20,000+ Reviews Analyzed</span>
              </div>
              <div className="flex items-center space-x-2 animate-float delay-100">
                <CheckCircle2 className="h-5 w-5 text-green-500" />
                <span>AI Insights in Seconds</span>
              </div>
              <div className="flex items-center space-x-2 animate-float delay-200">
                <CheckCircle2 className="h-5 w-5 text-green-500" />
                <span>Zero Training Required</span>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Features Grid */}
      <section className="py-20 px-4 sm:px-6 lg:px-8 bg-white/50 backdrop-blur-sm">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-16 animate-fade-in-up delay-500 opacity-0">
            <h2 className="text-4xl font-bold text-gray-900 mb-4">
              Everything You Need, Nothing You Don't
            </h2>
            <p className="text-lg text-gray-600 max-w-2xl mx-auto">
              Powerful analytics that feel effortless — designed for decision-makers, not data scientists
            </p>
          </div>

          <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-8">
            {features.map((feature, index) => (
              <div
                key={index}
                className="group p-6 bg-gradient-to-br from-gray-50 to-white border border-gray-200 rounded-2xl hover:shadow-xl hover:border-blue-300 transition-all duration-300 animate-fade-in-up opacity-0"
                style={{ animationDelay: `${0.6 + index * 0.1}s` }}
              >
                <div className="bg-gradient-to-br from-blue-500 to-indigo-500 w-12 h-12 rounded-xl flex items-center justify-center text-white mb-4 group-hover:scale-110 transition-transform">
                  {feature.icon}
                </div>
                <h3 className="text-xl font-semibold text-gray-900 mb-2">
                  {feature.title}
                </h3>
                <p className="text-gray-600 leading-relaxed">
                  {feature.description}
                </p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Agent Capabilities Section */}
      <section className="py-20 px-4 sm:px-6 lg:px-8 bg-gradient-to-br from-blue-50 to-indigo-50">
        <div className="max-w-6xl mx-auto">
          <div className="text-center mb-12">
            <div className="inline-flex items-center space-x-2 bg-indigo-100 border border-indigo-300 rounded-full px-4 py-2 text-indigo-700 text-sm font-semibold mb-4">
              <Sparkles className="h-4 w-4 animate-sparkle" />
              <span>Ask Anything, Get Everything</span>
            </div>
            <h2 className="text-4xl font-bold text-gray-900 mb-4">
              From Simple Questions to Deep Insights
            </h2>
            <p className="text-lg text-gray-600 max-w-3xl mx-auto">
              Whether you need a quick answer or complex statistical analysis, just ask naturally. 
              The agent figures out the rest — routing queries intelligently between semantic search and advanced analytics.
            </p>
          </div>

          <div className="space-y-4 mb-12">
            {agentQueries.map((item, index) => (
              <div
                key={index}
                className="group bg-white p-6 rounded-xl border border-gray-200 hover:border-indigo-400 hover:shadow-lg transition-all duration-300"
              >
                <div className="flex items-start space-x-4">
                  <div className="bg-indigo-100 text-indigo-600 w-10 h-10 rounded-lg flex items-center justify-center font-bold flex-shrink-0 group-hover:bg-indigo-600 group-hover:text-white transition-colors">
                    {index + 1}
                  </div>
                  <div className="flex-1">
                    <p className="text-gray-800 font-medium mb-2 text-lg">
                      "{item.query}"
                    </p>
                    <div className="flex items-start space-x-2 text-sm text-gray-600">
                      <Lightbulb className="h-4 w-4 text-yellow-500 mt-0.5 flex-shrink-0 animate-sparkle" />
                      <span className="italic">{item.insight}</span>
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>

          <div className="bg-gradient-to-br from-purple-50 to-pink-50 border-2 border-purple-200 rounded-2xl p-8 shadow-lg">
            <div className="flex items-start space-x-4">
              <div className="bg-gradient-to-br from-purple-500 to-pink-500 p-3 rounded-xl flex-shrink-0">
                <Brain className="h-6 w-6 text-white" />
              </div>
              <div>
                <h3 className="text-xl font-bold text-gray-900 mb-2">
                  Two Powerful Modes, Seamlessly Integrated
                </h3>
                <p className="text-gray-700 leading-relaxed mb-4">
                  <span className="font-semibold text-purple-700">Standard Mode:</span> Lightning-fast semantic search with comprehensive statistics, 
                  distributions, and correlations across all your reviews.
                </p>
                <p className="text-gray-700 leading-relaxed">
                  <span className="font-semibold text-purple-700">Advanced Agent Mode:</span> Natural language queries automatically trigger 
                  intelligent analytical functions — the agent interprets your question, picks the right approach, and delivers 
                  deep statistical breakdowns with stunning visualizations. All powered by MindsDB's smart query routing.
                </p>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Smart Sampling Logic Section - REDESIGNED */}
      <section className="py-20 px-4 sm:px-6 lg:px-8 bg-white/50">
        <div className="max-w-6xl mx-auto">
          <div className="text-center mb-12">
            <div className="inline-flex items-center space-x-2 bg-amber-50 border border-amber-300 rounded-full px-4 py-2 text-amber-700 text-sm font-semibold mb-4">
              <Target className="h-4 w-4 animate-sparkle" />
              <span>Statistical Precision</span>
            </div>
            <h2 className="text-4xl font-bold text-gray-900 mb-4">
              Built on Real Statistical Grounding
            </h2>
            <p className="text-lg text-gray-600 max-w-3xl mx-auto">
              Every metric you see is calculated from semantically matched reviews — ensuring results remain 
              context-aware, focused, and statistically meaningful.
            </p>
          </div>

          <div className="grid md:grid-cols-3 gap-6">
            <div className="bg-gradient-to-br from-emerald-50 to-teal-50 p-8 rounded-2xl border-2 border-emerald-200 shadow-lg">
              <div className="bg-gradient-to-br from-emerald-500 to-teal-600 w-14 h-14 rounded-xl flex items-center justify-center text-white mb-4">
                <BarChart3 className="h-7 w-7" />
              </div>
              <h3 className="text-xl font-bold text-gray-900 mb-3">Top N Sampling</h3>
              <p className="text-gray-700 leading-relaxed">
                Metrics are calculated from the <span className="font-semibold text-emerald-700">top 100 semantically matched reviews</span> — 
                the ones most relevant to your specific query.
              </p>
            </div>

            <div className="bg-gradient-to-br from-blue-50 to-indigo-50 p-8 rounded-2xl border-2 border-blue-200 shadow-lg">
              <div className="bg-gradient-to-br from-blue-500 to-indigo-600 w-14 h-14 rounded-xl flex items-center justify-center text-white mb-4">
                <Target className="h-7 w-7" />
              </div>
              <h3 className="text-xl font-bold text-gray-900 mb-3">Context-Aware</h3>
              <p className="text-gray-700 leading-relaxed">
                Rather than analyzing all 20,000+ reviews, we focus on <span className="font-semibold text-blue-700">what matters most</span> to 
                your question — delivering precise, actionable insights.
              </p>
            </div>

            <div className="bg-gradient-to-br from-purple-50 to-pink-50 p-8 rounded-2xl border-2 border-purple-200 shadow-lg">
              <div className="bg-gradient-to-br from-purple-500 to-pink-600 w-14 h-14 rounded-xl flex items-center justify-center text-white mb-4">
                <Zap className="h-7 w-7" />
              </div>
              <h3 className="text-xl font-bold text-gray-900 mb-3">Statistically Valid</h3>
              <p className="text-gray-700 leading-relaxed">
                Every percentage, distribution, and correlation is <span className="font-semibold text-purple-700">grounded in real data</span> — 
                not guesswork or assumptions.
              </p>
            </div>
          </div>

          <div className="mt-8 bg-gradient-to-br from-gray-50 to-slate-50 border-2 border-gray-200 rounded-2xl p-8 shadow-lg">
            <div className="flex items-start space-x-4">
              <div className="bg-gradient-to-br from-gray-600 to-slate-700 p-3 rounded-xl flex-shrink-0">
                <CheckCircle2 className="h-6 w-6 text-white" />
              </div>
              <div>
                <h3 className="text-xl font-bold text-gray-900 mb-2">
                  Why This Matters
                </h3>
                <p className="text-gray-700 leading-relaxed">
                  Generic dashboards show you everything. We show you what's <span className="font-semibold">relevant</span>. 
                  By focusing on semantically matched reviews, you get insights that directly answer your question — 
                  not diluted statistics from unrelated feedback.
                </p>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* InsightInterpreter Section - REDESIGNED */}
      <section className="py-20 px-4 sm:px-6 lg:px-8 ">
        <div className="max-w-6xl mx-auto">
          <div className="text-center mb-12">
            <div className="inline-flex items-center space-x-2 bg-emerald-100 border border-emerald-300 rounded-full px-4 py-2 text-emerald-700 text-sm font-semibold mb-4">
              <Lightbulb className="h-4 w-4 animate-sparkle" />
              <span>Beyond Numbers</span>
            </div>
            <h2 className="text-4xl font-bold text-gray-900 mb-4">
              Meet InsightInterpreter: Your AI Analyst
            </h2>
            <p className="text-lg text-gray-600 max-w-3xl mx-auto">
              Data is great. Knowing what to <span className="font-semibold text-emerald-700">do</span> with it is better. 
              InsightInterpreter spots hidden trends and turns them into strategic actions.
            </p>
          </div>

          <div className="grid md:grid-cols-2 gap-8 mb-8">
            <div className="bg-white rounded-2xl p-8 shadow-lg border-2 border-red-100">
              <div className="flex items-center gap-3 mb-6">
                <div className="w-14 h-14 rounded-xl bg-gradient-to-br from-red-500 to-rose-500 flex items-center justify-center text-white">
                  <TrendingDown className="h-7 w-7" />
                </div>
                <h3 className="text-2xl font-bold text-gray-900">Most Analytics Tools</h3>
              </div>
              <div className="space-y-4 mb-6">
                <div className="flex items-start gap-3 bg-red-50 p-4 rounded-lg border border-red-100">
                  <span className="text-red-500 font-bold text-xl flex-shrink-0">×</span>
                  <span className="text-gray-700">"Average rating is 6.2 out of 10"</span>
                </div>
                <div className="flex items-start gap-3 bg-red-50 p-4 rounded-lg border border-red-100">
                  <span className="text-red-500 font-bold text-xl flex-shrink-0">×</span>
                  <span className="text-gray-700">"48% of users recommended"</span>
                </div>
                <div className="flex items-start gap-3 bg-red-50 p-4 rounded-lg border border-red-100">
                  <span className="text-red-500 font-bold text-xl flex-shrink-0">×</span>
                  <span className="text-gray-700">"Seat comfort rated 3.5 stars"</span>
                </div>
              </div>
              <div className="bg-red-50 border-l-4 border-red-500 p-4 rounded">
                <p className="text-sm text-red-700 font-semibold">Numbers alone don't tell you what to fix.</p>
              </div>
            </div>

            <div className="bg-gradient-to-br from-emerald-50 to-teal-50 rounded-2xl p-8 shadow-lg border-2 border-emerald-300">
              <div className="flex items-center gap-3 mb-6">
                <div className="w-14 h-14 rounded-xl bg-gradient-to-br from-emerald-500 to-teal-600 flex items-center justify-center text-white">
                  <TrendingUp className="h-7 w-7" />
                </div>
                <h3 className="text-2xl font-bold text-gray-900">InsightInterpreter</h3>
              </div>
              <div className="space-y-4 mb-6">
                <div className="flex items-start gap-3 bg-white p-4 rounded-lg border-2 border-emerald-200 shadow-sm">
                  <span className="text-emerald-600 font-bold text-xl flex-shrink-0">✓</span>
                  <span className="text-gray-800 font-medium">"Wi-Fi below 3 stars cuts total satisfaction by 68%. Fix routers, not recipes."</span>
                </div>
                <div className="flex items-start gap-3 bg-white p-4 rounded-lg border-2 border-emerald-200 shadow-sm">
                  <span className="text-emerald-600 font-bold text-xl flex-shrink-0">✓</span>
                  <span className="text-gray-800 font-medium">"Business travelers forgive delays but hate dirty cabins. Prioritize cleaning staff."</span>
                </div>
                <div className="flex items-start gap-3 bg-white p-4 rounded-lg border-2 border-emerald-200 shadow-sm">
                  <span className="text-emerald-600 font-bold text-xl flex-shrink-0">✓</span>
                  <span className="text-gray-800 font-medium">"High ratings + low recommendations = good service, bad loyalty. Rethink pricing."</span>
                </div>
              </div>
              <div className="bg-emerald-100 border-l-4 border-emerald-600 p-4 rounded">
                <p className="text-sm text-emerald-800 font-semibold">Finally, AI that tells you *why* and what to do next.</p>
              </div>
            </div>
          </div>

          <div className="bg-white rounded-2xl p-8 shadow-lg border-2 border-emerald-200">
            <div className="flex items-start gap-4 mb-6">
              <div className="w-14 h-14 rounded-xl bg-gradient-to-br from-emerald-500 to-teal-600 flex items-center justify-center text-white flex-shrink-0">
                <Brain className="h-7 w-7" />
              </div>
              <div className="flex-1">
                <h3 className="text-2xl font-bold text-gray-900 mb-3">
                  How It Works
                </h3>
                <p className="text-gray-700 leading-relaxed text-lg">
                  After running any query, semantic or analytical, hit "Get AI Insights." InsightInterpreter reviews your results and delivers real intelligence, not summaries.
                </p>
              </div>
            </div>

            <div className="grid md:grid-cols-2 gap-6">
              <div className="flex items-start gap-3 p-4 bg-emerald-50 rounded-lg border border-emerald-200">
                <CheckCircle2 className="h-6 w-6 text-emerald-600 mt-0.5 flex-shrink-0" />
                <div>
                  <p className="font-semibold text-gray-900 text-base mb-1">Hidden Patterns</p>
                  <p className="text-sm text-gray-600">Uncover correlations and contradictions behind scores</p>
                </div>
              </div>
              <div className="flex items-start gap-3 p-4 bg-emerald-50 rounded-lg border border-emerald-200">
                <CheckCircle2 className="h-6 w-6 text-emerald-600 mt-0.5 flex-shrink-0" />
                <div>
                  <p className="font-semibold text-gray-900 text-base mb-1">Root Causes</p>
                  <p className="text-sm text-gray-600">Pinpoint what truly impacts satisfaction and loyalty</p>
                </div>
              </div>
              <div className="flex items-start gap-3 p-4 bg-emerald-50 rounded-lg border border-emerald-200">
                <CheckCircle2 className="h-6 w-6 text-emerald-600 mt-0.5 flex-shrink-0" />
                <div>
                  <p className="font-semibold text-gray-900 text-base mb-1">Action Steps</p>
                  <p className="text-sm text-gray-600">From strategy to operations — clear next moves</p>
                </div>
              </div>
              <div className="flex items-start gap-3 p-4 bg-emerald-50 rounded-lg border border-emerald-200">
                <CheckCircle2 className="h-6 w-6 text-emerald-600 mt-0.5 flex-shrink-0" />
                <div>
                  <p className="font-semibold text-gray-900 text-base mb-1">Executive Clarity</p>
                  <p className="text-sm text-gray-600">Actionable insights written in plain, decision-level language</p>
                </div>
              </div>
            </div>
          </div>

          <div className="text-center mt-12">
            <a
              href="/home"
              className="inline-flex items-center space-x-2 bg-gradient-to-r from-emerald-600 to-teal-600 text-white px-10 py-5 rounded-xl font-semibold text-lg shadow-lg hover:shadow-xl transform hover:scale-105 transition-all duration-300"
            >
              <span>Start Analyzing Now</span>
              <ArrowRight className="h-6 w-6" />
            </a>
          </div>
        </div>
      </section>

      <Footer />
    </div>
  );
};

export default Landing;