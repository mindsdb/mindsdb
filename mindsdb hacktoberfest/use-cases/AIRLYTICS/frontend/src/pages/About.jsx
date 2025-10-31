import { 
  Database, 
  Brain, 
  Zap, 
  Code, 
  Sparkles, 
  Target,
  Rocket,
  Users,
  TrendingUp,
  MessageSquare,
  GitBranch,
  Layers,
  Cpu,
  RefreshCw,
  Lightbulb,
  Network,
  BarChart3
} from 'lucide-react';
import { useEffect } from 'react';
import Navbar from '../components/Navbar';
import Footer from '../components/Footer';

const About = () => {
  useEffect(() => {
    // Scroll to top on component mount
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }, []);

  const techStack = [
    {
      icon: <Brain className="h-8 w-8" />,
      name: "MindsDB Knowledge Bases",
      description: "Hybrid semantic + structured search using embeddings with metadata filtering for intelligent query interpretation"
    },
    {
      icon: <Network className="h-8 w-8" />,
      name: "Analytics Query Agent",
      description: "Intelligent agent that maps natural language to 5 analytical functions with 10,000+ metric combinations"
    },
    {
      icon: <Lightbulb className="h-8 w-8" />,
      name: "Decision Intelligence Agent",
      description: "Management-focused agent providing actionable recommendations from analytical findings"
    },
    {
      icon: <Zap className="h-8 w-8" />,
      name: "FastAPI Backend",
      description: "High-performance async Python backend with NumPy, Pandas, SciPy for statistical computation"
    },
    {
      icon: <Code className="h-8 w-8" />,
      name: "React + Vite + Tailwind",
      description: "Modern frontend stack with component-based architecture and utility-first styling"
    },
    {
      icon: <RefreshCw className="h-8 w-8" />,
      name: "MindsDB Job Scheduling",
      description: "Automated dataset updates through scheduled jobs that sync new reviews into knowledge base"
    }
  ];

  const architectureFlow = [
    {
      step: "01",
      title: "Natural Language Input",
      description: "User asks a question in plain English about airline reviews, ratings, or service quality",
      icon: <MessageSquare className="h-6 w-6" />,
      tech: "React UI → FastAPI"
    },
    {
      step: "02",
      title: "Agent Interpretation",
      description: "Analytics Query Agent analyzes intent and routes to appropriate analytical function (conditional rating, distribution, correlation, etc.)",
      icon: <Brain className="h-6 w-6" />,
      tech: "MindsDB Agent Layer"
    },
    {
      step: "03",
      title: "Hybrid Search Execution",
      description: "Knowledge Base performs semantic search on 20K+ reviews while filtering by structured metadata (ratings, seat type, traveler type)",
      icon: <Database className="h-6 w-6" />,
      tech: "MindsDB KB + Embeddings"
    },
    {
      step: "04",
      title: "Statistical Processing",
      description: "Backend computes correlations, distributions, percentages, and cross-metric analytics using NumPy/Pandas based on retrieved top reviews.",
      icon: <TrendingUp className="h-6 w-6" />,
      tech: "FastAPI + SciPy"
    },
    {
      step: "05",
      title: "Decision Intelligence",
      description: "Decision Agent interprets findings and generates management-ready recommendations and action items",
      icon: <Lightbulb className="h-6 w-6" />,
      tech: "MindsDB Decision Agent"
    },
    {
      step: "06",
      title: "Visual Response",
      description: "Interactive charts, summary statistics, top reviews, and strategic insights rendered in real-time",
      icon: <BarChart3 className="h-6 w-6" />,
      tech: "React + Recharts"
    }
  ];

  const analyticalFunctions = [
    {
      name: "conditional_rating_analysis",
      description: "Analyzes rating patterns within specific conditions",
      example: "Among users who mentioned poor meals, what percent rated food above 4 and recommended the airline?"
    },
    {
      name: "conditional_rating_to_rating_analysis",
      description: "Cross-analyzes two rating dimensions under conditions",
      example: "Users with baggage delays — what % rated ground service above 4 but overall below 5?"
    },
    {
      name: "conditional_category_to_category_analysis",
      description: "Compares categorical distributions under conditions",
      example: "Among crowded flight reviews, how many Economy passengers were Solo vs Business travelers?"
    },
    {
      name: "conditional_distribution_analysis",
      description: "Shows distribution patterns within filtered subsets",
      example: "For check-in delay complaints, show seat type distribution among those rating ground service > 3"
    },
    {
      name: "general_percentage_distribution",
      description: "Calculates percentage breakdowns across conditions",
      example: "Of all slow boarding reviews, what percentage rated value for money above 4?"
    }
  ];

  const dataSchema = [
    { field: "overall_rating", type: "Integer (1-10)", description: "Overall satisfaction score" },
    { field: "seat_comfort", type: "Integer (1-5)", description: "Comfort rating for seating" },
    { field: "cabin_staff_service", type: "Integer (1-5)", description: "Flight attendant service quality" },
    { field: "food_beverages", type: "Integer (1-5)", description: "In-flight meal and drink rating" },
    { field: "ground_service", type: "Integer (1-5)", description: "Check-in, boarding, baggage service" },
    { field: "wifi_connectivity", type: "Integer (1-5)", description: "Internet connectivity quality" },
    { field: "value_for_money", type: "Integer (1-5)", description: "Price-to-experience ratio" },
    { field: "type_of_traveller", type: "Category", description: "Solo/Couple Leisure, Family, Business" },
    { field: "seat_type", type: "Category", description: "Economy, Premium Economy, Business, First" },
    { field: "recommended", type: "Boolean", description: "Would recommend airline (yes/no)" },
    { field: "verified", type: "Boolean", description: "Verified purchase status" },
    { field: "review_text", type: "Text", description: "Natural language customer feedback" }
  ];

  const realWorldUse = [
    {
      icon: <Target className="h-6 w-6" />,
      title: "Operational Intelligence",
      description: "Identify service areas (Wi-Fi, staff, food) that correlate with overall satisfaction for targeted improvements"
    },
    {
      icon: <Users className="h-6 w-6" />,
      title: "Customer Segmentation",
      description: "Map sentiment clusters across traveler types and seat classes to personalize service delivery"
    },
    {
      icon: <TrendingUp className="h-6 w-6" />,
      title: "Reputation Monitoring",
      description: "Detect emerging pain points across airlines, routes, and service categories in real-time"
    },
    {
      icon: <Lightbulb className="h-6 w-6" />,
      title: "Strategic Decision Making",
      description: "Get explainable, actionable recommendations from AI that management can trust and act upon"
    }
  ];

  const technicalFeatures = [
    "Zero-ETL analytics: Data-in-place processing without data transformation pipelines",
    "RAG-style intelligence: Retrieval-Augmented Generation for context-aware analysis",
    "Fallback mechanism: Agent intelligently switches to base search when queries are ambiguous",
    "Automated data ingestion: MindsDB job scheduling keeps knowledge base updated with new reviews",
    "Explainable AI: Transparent analytical process with traceable logic from query to insight",
    "Scalable architecture: Handles 20K+ documents with sub-second response times"
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

        @keyframes slideInFromBottom {
          from {
            opacity: 0;
            transform: translateY(100px);
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
            transform: rotate(5deg) scale(1.01);
            opacity: 0.8;
          }
        }

        @keyframes rotate {
          from {
            transform: rotate(0deg);
          }
          to {
            transform: rotate(360deg);
          }
        }

        @keyframes float {
          0%, 100% {
            transform: translateY(0px);
          }
          50% {
            transform: translateY(-3px);
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

        .animate-slide-in {
          animation: slideInFromBottom 1s ease-out forwards;
        }

        .animate-sparkle {
          animation: sparkle 3s ease-in-out infinite;
        }

        .animate-rotate {
          animation: rotate 8s linear infinite;
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
        .delay-700 { animation-delay: 0.7s; }
        .delay-800 { animation-delay: 0.8s; }
      `}</style>

      <Navbar />
      
      {/* Hero Section */}
      <section className="pt-32 pb-10 px-4 sm:px-6 lg:px-8">
        <div className="max-w-5xl mx-auto text-center space-y-6">
          <div className="inline-flex items-center space-x-2 bg-indigo-50 border border-indigo-200 rounded-full px-4 py-2 text-indigo-700 text-sm font-medium animate-fade-in-up opacity-0">
            <Target className="h-4 w-4 animate-sparkle" />
            <span>Technical Deep Dive</span>
          </div>
          
          <h1 className="text-5xl sm:text-5xl font-bold text-gray-900 leading-tight animate-fade-in-up delay-100 opacity-0">
            Turning Passengers' Voices
            <br />
            <span className="bg-gradient-to-r from-blue-600 to-indigo-600 bg-clip-text text-transparent">
              Into Data-Driven Stories
            </span>
          </h1>
          
          <p className="text-xl text-gray-600 max-w-3xl mx-auto leading-relaxed animate-fade-in-up delay-200 opacity-0">
            AirLytics demonstrates how Knowledge Bases transform messy, text-heavy customer feedback 
            into business-ready intelligence using RAG-style analytics and explainable AI.
          </p>
        </div>
      </section>

      {/* Purpose Section */}
      <section className="px-4 sm:px-6 lg:px-8">
        <div className="max-w-5xl mx-auto">
          <div className="bg-gradient-to-br from-blue-50 to-indigo-50 rounded-3xl p-8 md:p-12 border border-blue-100 animate-slide-in delay-300 opacity-0">
            <div className="flex items-start space-x-4 mb-6">
              <div className="bg-gradient-to-br from-blue-600 to-indigo-600 p-3 rounded-xl animate-pulse-glow">
                <Target className="h-6 w-6 text-white animate-float" />
              </div>
              <h2 className="text-3xl font-bold text-gray-900 mt-1">Built for Airline Management</h2>
            </div>
            
            <div className="space-y-4 text-gray-700 leading-relaxed text-lg">
              <p>
                AirLytics bridges the gap between raw customer feedback and strategic business decisions. 
                Traditional analytics tools require SQL expertise, data engineering, and technical teams. 
                This creates friction between insights and action.
              </p>
              <p>
                <span className="font-semibold text-gray-900">We eliminate that friction.</span> Using 
                MindsDB's Knowledge Base architecture with dual-agent intelligence, airline executives can 
                ask complex analytical questions in natural language and receive not just data, but 
                <span className="font-semibold text-gray-900"> decision-ready recommendations</span>.
              </p>
              <p>
                Our Decision Intelligence Agent interprets analytical findings through a management lens — 
                mapping data patterns to operational actions, identifying service improvement priorities, 
                and suggesting strategic responses. It's analytics that speaks business language.
              </p>
            </div>
          </div>
        </div>
      </section>

      {/* Architecture Flow */}
      <section className="py-16 px-4 sm:px-6 lg:px-8 bg-white/50 backdrop-blur-sm">
        <div className="max-w-6xl mx-auto">
          <div className="text-center mb-12 animate-fade-in-up delay-400 opacity-0">
            <h2 className="text-4xl font-bold text-gray-900 mb-4">System Architecture</h2>
            <p className="text-lg text-gray-600">
              End-to-end flow from natural language query to actionable insight
            </p>
          </div>

          <div className="grid md:grid-cols-2 gap-6">
            {architectureFlow.map((item, index) => (
              <div
                key={index}
                className="relative bg-gradient-to-br from-gray-50 to-white border-2 border-gray-200 rounded-2xl p-6 hover:shadow-xl hover:border-blue-300 transition-all duration-300 animate-fade-in-up opacity-0"
                style={{ animationDelay: `${0.5 + index * 0.1}s` }}
              >
                <div className="flex items-start space-x-4">
                  <div className="relative">
                    <div className="text-5xl font-bold text-blue-100 absolute -top-4 -left-1">
                      {item.step}
                    </div>
                    <div className="relative bg-gradient-to-br from-blue-500 to-indigo-500 w-12 h-12 rounded-xl flex items-center justify-center text-white z-10 animate-pulse-glow">
                      <div className="animate-float">
                        {item.icon}
                      </div>
                    </div>
                  </div>
                  
                  <div className="flex-1 pt-1">
                    <h3 className="text-lg font-bold text-gray-900 mb-1">
                      {item.title}
                    </h3>
                    <p className="text-gray-600 text-sm leading-relaxed mb-2">
                      {item.description}
                    </p>
                    <span className="inline-block text-xs font-mono bg-blue-50 text-blue-600 px-2 py-1 rounded">
                      {item.tech}
                    </span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      Analytical Functions
      <section className="py-16 px-4 sm:px-6 lg:px-8 bg-gradient-to-br from-indigo-50 to-purple-50">
        <div className="max-w-6xl mx-auto">
          <div className="text-center mb-12">
            <div className="inline-flex items-center space-x-2 bg-indigo-100 border border-indigo-300 rounded-full px-4 py-2 text-indigo-700 text-sm font-semibold mb-4">
              <Cpu className="h-4 w-4 animate-rotate" />
              <span>Core Analytics Engine</span>
            </div>
            <h2 className="text-4xl font-bold text-gray-900 mb-4">Five Functions, Infinite Combinations</h2>
            <p className="text-lg text-gray-600 max-w-3xl mx-auto">
              Each function can analyze any combination of metadata fields, creating 10,000+ unique analytical paths
            </p>
          </div>

          <div className="space-y-4">
            {analyticalFunctions.map((func, index) => (
              <div
                key={index}
                className="bg-white border-2 border-indigo-200 rounded-xl p-6 hover:shadow-lg hover:border-indigo-400 transition-all duration-300"
              >
                <div className="flex items-start space-x-4">
                  <div className="bg-gradient-to-br from-indigo-500 to-purple-500 text-white w-10 h-10 rounded-lg flex items-center justify-center font-bold flex-shrink-0">
                    {index + 1}
                  </div>
                  <div className="flex-1">
                    <h3 className="text-lg font-bold text-gray-900 mb-1 font-mono">
                      {func.name}
                    </h3>
                    <p className="text-gray-700 mb-2">
                      {func.description}
                    </p>
                    <div className="bg-gray-50 border border-gray-200 rounded-lg p-3 text-sm text-gray-600 italic">
                      "{func.example}"
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Technology Stack */}
      <section className="py-16 px-4 sm:px-6 lg:px-8">
        <div className="max-w-6xl mx-auto">
          <div className="text-center mb-12">
            <h2 className="text-4xl font-bold text-gray-900 mb-4">Technology Stack</h2>
            <p className="text-lg text-gray-600">
              Production-grade tools for enterprise AI analytics
            </p>
          </div>

          <div className="grid md:grid-cols-2 gap-6">
            {techStack.map((tech, index) => (
              <div
                key={index}
                className="group bg-white border-2 border-gray-200 rounded-2xl p-6 hover:border-blue-400 hover:shadow-lg transition-all duration-300"
              >
                <div className="flex items-start space-x-4">
                  <div className="bg-gradient-to-br from-blue-500 to-indigo-500 p-3 rounded-xl text-white group-hover:scale-110 transition-transform flex-shrink-0">
                    <div className="animate-sparkle">
                      {tech.icon}
                    </div>
                  </div>
                  <div className="flex-1">
                    <h3 className="text-lg font-bold text-gray-900 mb-2">
                      {tech.name}
                    </h3>
                    <p className="text-gray-600 leading-relaxed text-sm">
                      {tech.description}
                    </p>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Data Schema */}
      <section className="py-16 px-4 sm:px-6 lg:px-8 bg-gray-50">
        <div className="max-w-6xl mx-auto">
          <div className="text-center mb-12">
            <h2 className="text-4xl font-bold text-gray-900 mb-4">Dataset Schema</h2>
            <p className="text-lg text-gray-600">
              20,000+ airline reviews with rich structured metadata
            </p>
          </div>

          <div className="bg-white rounded-2xl border-2 border-gray-200 overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gradient-to-r from-blue-50 to-indigo-50">
                  <tr>
                    <th className="px-6 py-4 text-left text-sm font-semibold text-gray-900">Field Name</th>
                    <th className="px-6 py-4 text-left text-sm font-semibold text-gray-900">Type</th>
                    <th className="px-6 py-4 text-left text-sm font-semibold text-gray-900">Description</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {dataSchema.map((field, index) => (
                    <tr key={index} className="hover:bg-blue-50 transition-colors">
                      <td className="px-6 py-4 text-sm font-mono text-indigo-600 font-semibold">
                        {field.field}
                      </td>
                      <td className="px-6 py-4 text-sm text-gray-600">
                        {field.type}
                      </td>
                      <td className="px-6 py-4 text-sm text-gray-700">
                        {field.description}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </section>

      {/* Real-World Applications */}
      <section className="py-16 px-4 sm:px-6 lg:px-8">
        <div className="max-w-6xl mx-auto">
          <div className="text-center mb-12">
            <h2 className="text-4xl font-bold text-gray-900 mb-4">Real-World Impact</h2>
            <p className="text-lg text-gray-600">
              How airline management teams use AirLytics
            </p>
          </div>

          <div className="grid md:grid-cols-2 gap-6">
            {realWorldUse.map((use, index) => (
              <div
                key={index}
                className="bg-gradient-to-br from-white to-blue-50 border-2 border-blue-200 rounded-2xl p-6 hover:shadow-lg transition-all duration-300"
              >
                <div className="flex items-start space-x-4">
                  <div className="bg-gradient-to-br from-blue-500 to-indigo-500 p-3 rounded-xl text-white flex-shrink-0 animate-pulse-glow">
                    <div className="animate-float">
                      {use.icon}
                    </div>
                  </div>
                  <div>
                    <h3 className="text-lg font-bold text-gray-900 mb-2">
                      {use.title}
                    </h3>
                    <p className="text-gray-600 leading-relaxed text-sm">
                      {use.description}
                    </p>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Technical Features */}
      <section className="py-16 px-4 sm:px-6 lg:px-8 bg-gradient-to-br from-gray-50 to-blue-50">
        <div className="max-w-5xl mx-auto">
          <div className="text-center mb-12">
            <div className="inline-flex items-center space-x-2 bg-blue-100 border border-blue-300 rounded-full px-4 py-2 text-blue-700 text-sm font-semibold mb-4">
              <Layers className="h-4 w-4 animate-sparkle" />
              <span>Technical Highlights</span>
            </div>
            <h2 className="text-4xl font-bold text-gray-900 mb-4">Enterprise-Grade Features</h2>
          </div>

          <div className="grid md:grid-cols-2 gap-4">
            {technicalFeatures.map((feature, index) => (
              <div
                key={index}
                className="bg-white border border-gray-200 rounded-xl p-5 hover:border-blue-400 hover:shadow-md transition-all duration-300"
              >
                <div className="flex items-start space-x-3">
                  <div className="bg-blue-100 text-blue-600 w-6 h-6 rounded-lg flex items-center justify-center text-xs font-bold flex-shrink-0 mt-0.5">
                    {index + 1}
                  </div>
                  <p className="text-gray-700 leading-relaxed text-sm">
                    {feature}
                  </p>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Hacktoberfest Alignment */}
      <section className="py-16 px-4 sm:px-6 lg:px-8">
        <div className="max-w-5xl mx-auto">
          <div className="bg-gradient-to-br from-orange-50 to-red-50 rounded-3xl p-8 md:p-12 border-2 border-orange-200">
            <div className="flex items-start space-x-4 mb-6">
              <div className="bg-gradient-to-br from-orange-500 to-red-500 p-3 rounded-xl animate-pulse-glow">
                <Rocket className="h-6 w-6 text-white animate-float" />
              </div>
              <h2 className="text-3xl font-bold text-gray-900 mt-1">MindsDB Hacktoberfest 2025</h2>
            </div>
            
            <div className="space-y-3 text-gray-700">
              <p className="flex items-start space-x-2">
                <span className="text-orange-500 font-bold mt-1">✓</span>
                <span><strong>Track 1:</strong> Fully functional web app powered by MindsDB Knowledge Bases</span>
              </p>
              <p className="flex items-start space-x-2">
                <span className="text-orange-500 font-bold mt-1">✓</span>
                <span><strong>Track 2:</strong> Advanced features including metadata filtering, semantic+SQL hybrid search, and dual-agent integration</span>
              </p>
              <p className="flex items-start space-x-2">
                <span className="text-orange-500 font-bold mt-1">✓</span>
                <span><strong>Zero-ETL:</strong> Demonstrates data-in-place analytics without transformation pipelines</span>
              </p>
              <p className="flex items-start space-x-2">
                <span className="text-orange-500 font-bold mt-1">✓</span>
                <span><strong>Evaluate KB:</strong> Combines explainability, accuracy, and RAG-style analytics</span>
              </p>
              <p className="flex items-start space-x-2">
                <span className="text-orange-500 font-bold mt-1">✓</span>
                <span><strong>Enterprise Ready:</strong> Reference architecture for conversational AI analytics at scale</span>
              </p>
            </div>
          </div>
        </div>
      </section>

      {/* CTA */}
      <section className="py-16 px-4 sm:px-6 lg:px-8">
        <div className="max-w-4xl mx-auto text-center">
          <h2 className="text-3xl font-bold text-gray-900 mb-6">
            Ready to Transform Your Airline Data?
          </h2>
          <p className="text-lg text-gray-600 mb-8">
            Experience the future of conversational analytics
          </p>
          <a
            href="/home"
            className="inline-flex items-center space-x-2 bg-gradient-to-r from-blue-600 to-indigo-600 text-white px-10 py-5 rounded-xl font-semibold text-lg shadow-lg hover:shadow-xl transform hover:scale-105 transition-all duration-300 animate-pulse-glow"
          >
            <span>Start Analyzing Now</span>
            <Sparkles className="h-5 w-5 animate-sparkle" />
          </a>
        </div>
      </section>

      <Footer />
    </div>
  );
};

export default About;