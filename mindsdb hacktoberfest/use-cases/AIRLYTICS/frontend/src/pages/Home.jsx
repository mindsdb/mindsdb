// src/pages/Home.jsx

import { useState, useEffect } from 'react';
import {
  Search, Send, Sparkles, Settings, Zap, TrendingUp, BarChart3, Brain,
  Lightbulb, Filter, Loader2, AlertCircle, CheckCircle2, Users, Plane, Star, MessageSquare, HelpCircle
} from 'lucide-react';
import Navbar from '../components/Navbar';
import Footer from '../components/Footer';
import BaseCase from '../components/BaseCase';
import SpecialCase from '../components/SpecialCase';
import Interpreter from '../components/Interpreter';
import AgentHelp from '../components/AgentHelp';

const TOP_AIRLINES = [
  "Frontier Airlines", "Turkish Airlines", "Thomson Airways", "China Eastern Airlines", "China Southern Airlines",
  "AirAsia India", "Vietnam Airlines", "Air Serbia", "FlySafair", "Air India",
  "Norwegian", "United Airlines", "Oman Air", "Breeze Airways", "Transavia",
  "Singapore Airlines", "Air New Zealand", "PLAY", "Garuda Indonesia", "Air Berlin",
  "Iberia", "Finnair", "Royal Brunei Airlines", "Go First", "Virgin America",
  "CSA Czech Airlines", "Etihad Airways", "Korean Air", "Hawaiian Airlines", "Egyptair",
  "El Al Israel Airlines", "Hong Kong Airlines", "Thomas Cook Airlines", "easyJet", "Gulf Air",
  "Qatar Airways", "Air France", "Nok Air", "Thai Smile Airways", "Porter Airlines",
  "Virgin Australia", "Malindo Air", "Emirates", "Air Mauritius", "Hainan Airlines",
  "Jetstar Asia", "Delta Air Lines", "Tigerair", "Kuwait Airways", "Air Canada"
];

const Home = () => {
  const [query, setQuery] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [results, setResults] = useState(null);
  const [error, setError] = useState(null);
  const [showFilters, setShowFilters] = useState(false);
  const [apiMode, setApiMode] = useState('semantic');
  const [showHelp, setShowHelp] = useState(false);

  const [filters, setFilters] = useState({
    airline_name: '',
    type_of_traveller: '',
    seat_type: '',
    recommended: '',
    verified: null,
    overall_rating: null,
    seat_comfort: null,
    cabin_staff_service: null,
    food_beverages: null,
    ground_service: null,
    inflight_entertainment: null,
    wifi_connectivity: null,
    value_for_money: null,
    limit: 100
  });

  useEffect(() => {
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }, []);

  useEffect(() => {
    setResults(null);
    setError(null);
  }, [apiMode]);

  const buildRequestBody = (queryText) => {
    const requestBody = {
      query: queryText,
      limit: filters.limit
    };

    // Add metadata filters
    if (filters.airline_name) requestBody.airline_name = filters.airline_name;
    if (filters.type_of_traveller) requestBody.type_of_traveller = filters.type_of_traveller;
    if (filters.seat_type) requestBody.seat_type = filters.seat_type;
    if (filters.recommended) requestBody.recommended = filters.recommended;
    if (filters.verified !== null) requestBody.verified = filters.verified;

    // Add numeric filters
    [
      "overall_rating",
      "seat_comfort",
      "cabin_staff_service",
      "food_beverages",
      "ground_service",
      "inflight_entertainment",
      "wifi_connectivity",
      "value_for_money"
    ].forEach(key => {
      const val = filters[key];
      if (val !== null && val !== '') {
        const numVal = parseInt(val);
        if (!isNaN(numVal)) {
          requestBody[key] = numVal;
        }
      }
    });

    return requestBody;
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!query.trim()) return;

    setIsLoading(true);
    setError(null);
    setResults(null);

    try {
      if (apiMode === 'agent') {
        // Build request body with all filters for smart_case
        const smartRequestBody = buildRequestBody(query.trim());

        const smartResponse = await fetch('http://127.0.0.1:8000/smart_case', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(smartRequestBody)
        });

        if (!smartResponse.ok) throw new Error(`API Error: ${smartResponse.status}`);

        const smartData = await smartResponse.json();

        if (smartData.mode === 'special_case') {
          // 🔥 FIX: Use the REINTERPRETED query from agent, not original query
          const reinterpretedQuery = smartData.semantic_query_used || query.trim();
          
          // Build base_case request with the SAME reinterpreted query
          const baseRequestBody = buildRequestBody(reinterpretedQuery);
          
          const baseResponse = await fetch('http://127.0.0.1:8000/base_case', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(baseRequestBody)
          });

          if (baseResponse.ok) {
            const baseData = await baseResponse.json();
            setResults({
              ...smartData,
              baseCase: baseData
            });
          } else {
            setResults(smartData);
          }
        } else {
          setResults(smartData);
        }
      } else {
        // Semantic mode - build request with filters
        const requestBody = buildRequestBody(query.trim());

        const response = await fetch('http://127.0.0.1:8000/base_case', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(requestBody)
        });

        if (!response.ok) throw new Error(`API Error: ${response.status}`);

        const data = await response.json();
        setResults(data);
      }
    } catch (err) {
      setError(err.message || 'Failed to fetch results. Please try again.');
      console.error('API Error:', err);
    } finally {
      setIsLoading(false);
    }
  };

  const handleFilterChange = (field, value) => {
    const numericFieldKeys = [
      "overall_rating", "seat_comfort", "cabin_staff_service",
      "food_beverages", "ground_service", "inflight_entertainment",
      "wifi_connectivity", "value_for_money"
    ];

    if (numericFieldKeys.includes(field) && value === '') {
      value = null;
    }

    setFilters(prev => ({ ...prev, [field]: value }));
  };

  const clearFilters = () => {
    setFilters({
      airline_name: '',
      type_of_traveller: '',
      seat_type: '',
      recommended: '',
      verified: null,
      overall_rating: null,
      seat_comfort: null,
      cabin_staff_service: null,
      food_beverages: null,
      ground_service: null,
      inflight_entertainment: null,
      wifi_connectivity: null,
      value_for_money: null,
      limit: 100
    });
  };

  const exampleQueries = {
    semantic: [
      "delayed flight, late departure, missed connection, long waiting time",
      "lost baggage, delayed luggage, mishandled bag, missing suitcase",
      "friendly crew, helpful attendants, polite staff, great cabin service",
      "slow check-in, long queues, rude counter staff, chaotic boarding"
    ],
    agent: [
      "Out of all passengers mentioning uncomfortable seats, what % rated seat comfort above 3?",
      "For reviews complaining about rude staff, show cabin staff service distribution among travelers who rated overall experience below 6.",
      "Among passengers mentioning long layovers, compare the proportion of Business Class vs Economy travelers who were Solo travelers.",
      "Users mentioning noisy cabins, what % of those who rated seat comfort below 4 rated overall experience below 6?"
    ]
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 via-indigo-50 to-purple-50">
      <style>{`
        @keyframes fadeInUp {
          from { opacity: 0; transform: translateY(30px); }
          to { opacity: 1; transform: translateY(0); }
        }
        @keyframes sparkle {
          0%, 100% { transform: rotate(0deg) scale(1); opacity: 1; }
          50% { transform: rotate(180deg) scale(1.1); opacity: 0.8; }
        }
        @keyframes float {
          0%, 100% { transform: translateY(0px); }
          50% { transform: translateY(-3px); }
        }
        @keyframes pulse-glow {
          0%, 100% { box-shadow: 0 0 20px rgba(79, 70, 229, 0.3); }
          50% { box-shadow: 0 0 40px rgba(79, 70, 229, 0.5); }
        }
        .animate-fade-in-up { animation: fadeInUp 0.8s ease-out forwards; }
        .animate-sparkle { animation: sparkle 3s ease-in-out infinite; }
        .animate-float { animation: float 3s ease-in-out infinite; }
        .animate-pulse-glow { animation: pulse-glow 2s ease-in-out infinite; }
        .delay-100 { animation-delay: 0.1s; }
        .delay-200 { animation-delay: 0.2s; }
        .delay-300 { animation-delay: 0.3s; }
        .delay-400 { animation-delay: 0.4s; }
        .delay-500 { animation-delay: 0.5s; }
        .delay-600 { animation-delay: 0.6s; }
      `}</style>

      <Navbar />
      <div className="pt-24 pb-12 px-4 sm:px-6 lg:px-8">
        <div className="max-w-7xl mx-auto">
          {/* Header */}
          <div className="text-center mb-8">
            <div className="inline-flex items-center space-x-2 bg-blue-100/60 backdrop-blur-sm border border-blue-300 rounded-full px-4 py-2 mb-4 animate-fade-in-up opacity-0">
              <Sparkles className="h-4 w-4 text-blue-600 animate-sparkle" />
              <span className="text-sm font-medium text-blue-700">AI-Powered Review Analytics</span>
            </div>
            <h1 className="text-5xl font-bold pb-10 bg-gradient-to-r from-gray-900 via-blue-900 to-indigo-900 bg-clip-text text-transparent mb-3 animate-fade-in-up delay-100 opacity-0">
              Airline Reviews Intelligence
            </h1>
            <p className="text-lg text-gray-600 max-w-2xl mx-auto animate-fade-in-up delay-200 opacity-0">
              Search and analyze 20,000+ airline reviews with AI-powered insights
            </p>
          </div>

          {/* Mode Toggle + Help (Help now visible in both modes) */}
          <div className="max-w-4xl mx-auto mb-6 flex items-center justify-center gap-3 animate-fade-in-up delay-300 opacity-0">
            <div className="bg-white/80 backdrop-blur-sm rounded-xl shadow-lg p-1.5 border border-gray-200 inline-flex">
              <button
                onClick={() => setApiMode('semantic')}
                className={`px-6 py-3 rounded-lg font-semibold text-sm transition-all duration-300 flex items-center space-x-2 ${
                  apiMode === 'semantic'
                    ? 'bg-gradient-to-r from-blue-600 to-blue-700 text-white shadow-md transform scale-105'
                    : 'text-gray-600 hover:text-gray-900 hover:bg-gray-50'
                }`}
              >
                <Search className="h-4 w-4" />
                <span>Semantic Search</span>
              </button>

              <button
                onClick={() => setApiMode('agent')}
                className={`px-6 py-3 rounded-lg font-semibold text-sm transition-all duration-300 flex items-center space-x-2 ${
                  apiMode === 'agent'
                    ? 'bg-gradient-to-r from-indigo-600 to-purple-600 text-white shadow-md transform scale-105'
                    : 'text-gray-600 hover:text-gray-900 hover:bg-gray-50'
                }`}
              >
                <Brain className="h-4 w-4" />
                <span>Agent Analytics</span>
              </button>
            </div>

            {/* Help button always visible */}
            <button
              onClick={() => setShowHelp(true)}
              className="inline-flex items-center gap-2 px-4 py-3 rounded-lg text-sm font-semibold bg-white/80 border border-gray-200 hover:border-indigo-400 hover:shadow-md transition-all"
              title="See available fields and example analytical questions"
            >
              <HelpCircle className="h-4 w-4 text-indigo-600" />
              <span>Help</span>
            </button>
          </div>

          {/* Search Input */}
          <div className="max-w-4xl mx-auto mb-8 animate-fade-in-up delay-400 opacity-0">
            <div className="relative bg-white/90 backdrop-blur-md rounded-2xl shadow-2xl border-2 border-gray-200 hover:border-blue-300 transition-all duration-300 animate-pulse-glow">
              <div className="flex items-center p-4">
                <div className="pl-2 pr-4">
                  {isLoading ? (
                    <Brain className="h-6 w-6 text-indigo-500 animate-pulse" />
                  ) : apiMode === 'agent' ? (
                    <Brain className="h-6 w-6 text-indigo-500" />
                  ) : (
                    <Search className="h-6 w-6 text-blue-500" />
                  )}
                </div>
                <input
                  type="text"
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') {
                      e.preventDefault();
                      handleSubmit(e);
                    }
                  }}
                  placeholder={
                    apiMode === 'agent'
                      ? "Ask analytical questions like 'Among users who rated wifi < 3, what % had overall rating < 5?'"
                      : "Search reviews: 'Show me complaints about delayed flights'"
                  }
                  className="flex-1 px-2 py-3 text-base outline-none bg-transparent placeholder-gray-400"
                  disabled={isLoading}
                />
                <button
                  type="button"
                  onClick={() => setShowFilters(!showFilters)}
                  className={`p-2.5 rounded-lg transition-all mr-3 ${
                    showFilters
                      ? 'bg-blue-100 text-blue-600 shadow-sm'
                      : 'text-gray-400 hover:text-gray-600 hover:bg-gray-100'
                  }`}
                >
                  <Filter className="h-5 w-5" />
                </button>
                <button
                  type="button"
                  onClick={handleSubmit}
                  disabled={!query.trim() || isLoading}
                  className={`px-6 py-3 rounded-xl font-semibold shadow-lg transition-all duration-300 flex items-center space-x-2 disabled:opacity-50 disabled:cursor-not-allowed ${
                    apiMode === 'agent'
                      ? 'bg-gradient-to-r from-indigo-600 to-purple-600 text-white hover:shadow-xl hover:scale-105'
                      : 'bg-gradient-to-r from-blue-600 to-blue-700 text-white hover:shadow-xl hover:scale-105'
                  }`}
                >
                  {isLoading ? (
                    <>
                      <Loader2 className="h-4 w-4 animate-spin" />
                      <span>Processing</span>
                    </>
                  ) : (
                    <>
                      <span>{apiMode === 'agent' ? 'Analyze' : 'Search'}</span>
                      <Send className="h-4 w-4" />
                    </>
                  )}
                </button>
              </div>
            </div>

            {/* Filters Panel */}
            {showFilters && (
              <div className="mt-4 bg-white/90 backdrop-blur-md border-2 border-gray-200 rounded-2xl shadow-xl p-6">
                <div className="flex justify-between items-center mb-5">
                  <div className="flex items-center space-x-2">
                    <Settings className="h-5 w-5 text-gray-700" />
                    <h3 className="font-bold text-gray-900 text-lg">Advanced Filters</h3>
                  </div>
                  <button
                    onClick={clearFilters}
                    className="text-sm text-blue-600 hover:text-blue-700 font-semibold px-4 py-2 rounded-lg hover:bg-blue-50 transition-all duration-200"
                  >
                    Clear All
                  </button>
                </div>

                <div className="grid md:grid-cols-3 gap-4">
                  <div>
                    <label className="block text-sm font-semibold text-gray-700 mb-2 flex items-center space-x-1">
                      <Plane className="h-4 w-4 text-gray-500" />
                      <span>Airline</span>
                    </label>
                    <select
                      value={filters.airline_name}
                      onChange={(e) => handleFilterChange('airline_name', e.target.value)}
                      className="w-full px-4 py-2.5 border-2 border-gray-200 rounded-xl focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all text-sm bg-white"
                    >
                      <option value="">All Airlines</option>
                      {TOP_AIRLINES.map(a => (
                        <option key={a} value={a}>{a}</option>
                      ))}
                    </select>
                  </div>

                  <div>
                    <label className="block text-sm font-semibold text-gray-700 mb-2 flex items-center space-x-1">
                      <Users className="h-4 w-4 text-gray-500" />
                      <span>Traveler Type</span>
                    </label>
                    <select
                      value={filters.type_of_traveller}
                      onChange={(e) => handleFilterChange('type_of_traveller', e.target.value)}
                      className="w-full px-4 py-2.5 border-2 border-gray-200 rounded-xl focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all text-sm bg-white"
                    >
                      <option value="">All</option>
                      <option value="Solo Leisure">Solo Leisure</option>
                      <option value="Couple Leisure">Couple Leisure</option>
                      <option value="Family Leisure">Family Leisure</option>
                      <option value="Business">Business</option>
                    </select>
                  </div>

                  <div>
                    <label className="block text-sm font-semibold text-gray-700 mb-2">Seat Type</label>
                    <select
                      value={filters.seat_type}
                      onChange={(e) => handleFilterChange('seat_type', e.target.value)}
                      className="w-full px-4 py-2.5 border-2 border-gray-200 rounded-xl focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all text-sm bg-white"
                    >
                      <option value="">All Classes</option>
                      <option value="Economy Class">Economy Class</option>
                      <option value="Premium Economy">Premium Economy</option>
                      <option value="Business Class">Business Class</option>
                      <option value="First Class">First Class</option>
                    </select>
                  </div>

                  <div>
                    <label className="block text-sm font-semibold text-gray-700 mb-2 flex items-center space-x-1">
                      <CheckCircle2 className="h-4 w-4 text-gray-500" />
                      <span>Recommended</span>
                    </label>
                    <select
                      value={filters.recommended}
                      onChange={(e) => handleFilterChange('recommended', e.target.value)}
                      className="w-full px-4 py-2.5 border-2 border-gray-200 rounded-xl focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all text-sm bg-white"
                    >
                      <option value="">All</option>
                      <option value="yes">Yes</option>
                      <option value="no">No</option>
                    </select>
                  </div>

                  <div>
                    <label className="block text-sm font-semibold text-gray-700 mb-2">Verified</label>
                    <select
                      value={filters.verified === null ? '' : filters.verified.toString()}
                      onChange={(e) => handleFilterChange('verified', e.target.value === '' ? null : e.target.value === 'true')}
                      className="w-full px-4 py-2.5 border-2 border-gray-200 rounded-xl focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all text-sm bg-white"
                    >
                      <option value="">All</option>
                      <option value="true">Verified Only</option>
                      <option value="false">Unverified</option>
                    </select>
                  </div>

                  {[
                    { key: "overall_rating", label: "Overall Rating (1–10)", max: 10 },
                    { key: "seat_comfort", label: "Seat Comfort (1–5)", max: 5 },
                    { key: "cabin_staff_service", label: "Cabin Staff Service (1–5)", max: 5 },
                    { key: "food_beverages", label: "Food & Beverages (1–5)", max: 5 },
                    { key: "ground_service", label: "Ground Service (1–5)", max: 5 },
                    { key: "inflight_entertainment", label: "Inflight Entertainment (1–5)", max: 5 },
                    { key: "wifi_connectivity", label: "Wi-Fi Connectivity (1–5)", max: 5 },
                    { key: "value_for_money", label: "Value for Money (1–5)", max: 5 }
                  ].map(f => (
                    <div key={f.key}>
                      <label className="block text-sm font-semibold text-gray-700 mb-2 flex items-center space-x-1">
                        <Star className="h-4 w-4 text-gray-500" />
                        <span>{f.label}</span>
                      </label>
                      <input
                        type="number"
                        min="1"
                        max={f.max}
                        value={filters[f.key] === null ? '' : filters[f.key]}
                        onChange={(e) => handleFilterChange(f.key, e.target.value)}
                        placeholder={`1-${f.max}`}
                        className="w-full px-4 py-2.5 border-2 border-gray-200 rounded-xl focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all text-sm bg-white"
                      />
                    </div>
                  ))}

                  <div>
                    <label className="block text-sm font-semibold text-gray-700 mb-2">Result Limit</label>
                    <select
                      value={filters.limit}
                      onChange={(e) => handleFilterChange('limit', parseInt(e.target.value))}
                      className="w-full px-4 py-2.5 border-2 border-gray-200 rounded-xl focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all text-sm bg-white"
                    >
                      {[10,20, 50, 75, 100].map(l => (
                        <option key={l} value={l}>{l}</option>
                      ))}
                    </select>
                  </div>
                </div>
              </div>
            )}
          </div>

          {/* Initial State - Example Queries */}
          {!isLoading && !results && !error && (
            <div className="max-w-6xl mx-auto animate-fade-in-up delay-500 opacity-0">
              <div className="bg-white/80 backdrop-blur-md rounded-2xl shadow-xl p-8 border-2 border-gray-200">
                <div className="flex items-center space-x-2 mb-6">
                  <Lightbulb className="h-6 w-6 text-amber-500 animate-sparkle" />
                  <h3 className="text-xl font-bold text-gray-900">
                    Example Queries for {apiMode === 'agent' ? 'Agent Analytics' : 'Semantic Search'}
                  </h3>
                </div>

                <div className="grid md:grid-cols-2 gap-4 mb-8">
                  {exampleQueries[apiMode].map((example, idx) => (
                    <button
                      key={idx}
                      onClick={() => setQuery(example)}
                      className="group text-left p-5 bg-gradient-to-br from-gray-50 to-blue-50/30 border-2 border-gray-200 rounded-xl hover:border-blue-400 hover:shadow-lg transition-all duration-300 transform hover:scale-102"
                    >
                      <div className="flex items-start space-x-3">
                        <div className="flex-shrink-0 w-8 h-8 rounded-lg bg-gradient-to-br from-blue-500 to-indigo-500 text-white flex items-center justify-center text-sm font-bold shadow-md">
                          {idx + 1}
                        </div>
                        <p className="text-gray-700 text-sm font-medium group-hover:text-gray-900 leading-relaxed">
                          "{example}"
                        </p>
                      </div>
                    </button>
                  ))}
                </div>

                <div className="pt-6 border-t-2 border-gray-200">
                  <div className="grid md:grid-cols-3 gap-6">
                    <div className="flex items-start space-x-3 animate-float">
                      <div className="flex-shrink-0 w-12 h-12 rounded-xl bg-gradient-to-br from-blue-500 to-blue-600 flex items-center justify-center shadow-lg">
                        <MessageSquare className="h-6 w-6 text-white" />
                      </div>
                      <div>
                        <h4 className="font-bold text-gray-900 text-base mb-1">20K+ Reviews</h4>
                        <p className="text-sm text-gray-600">Comprehensive database</p>
                      </div>
                    </div>
                    <div className="flex items-start space-x-3 animate-float delay-100">
                      <div className="flex-shrink-0 w-12 h-12 rounded-xl bg-gradient-to-br from-indigo-500 to-purple-600 flex items-center justify-center shadow-lg">
                        <TrendingUp className="h-6 w-6 text-white" />
                      </div>
                      <div>
                        <h4 className="font-bold text-gray-900 text-base mb-1">10K+ Combinations</h4>
                        <p className="text-sm text-gray-600">Cross-metric patterns</p>
                      </div>
                    </div>
                    <div className="flex items-start space-x-3 animate-float delay-200">
                      <div className="flex-shrink-0 w-12 h-12 rounded-xl bg-gradient-to-br from-purple-500 to-pink-600 flex items-center justify-center shadow-lg">
                        <Brain className="h-6 w-6 text-white" />
                      </div>
                      <div>
                        <h4 className="font-bold text-gray-900 text-base mb-1">Dual Agents</h4>
                        <p className="text-sm text-gray-600">Analytics + Intelligence</p>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Loading State */}
          {isLoading && (
            <div className="text-center py-16">
              <div className="inline-flex flex-col items-center space-y-4 bg-white/90 backdrop-blur-md px-12 py-10 rounded-2xl shadow-2xl border-2 border-gray-200">
                <div className="relative">
                  <div className="absolute inset-0 bg-indigo-500 rounded-full opacity-20 animate-ping"></div>
                  <Brain className="w-12 h-12 text-indigo-600 animate-pulse relative" />
                </div>
                <span className="text-lg font-bold text-gray-900">
                  {apiMode === 'agent' ? 'Analyzing your query...' : 'Searching reviews...'}
                </span>
                <span className="text-sm text-gray-500">
                  Please wait while we process your request
                </span>
              </div>
            </div>
          )}

          {/* Error State */}
          {error && (
            <div className="max-w-4xl mx-auto">
              <div className="bg-red-50/90 backdrop-blur-md border-2 border-red-300 rounded-2xl p-6 shadow-xl">
                <div className="flex items-start space-x-3">
                  <AlertCircle className="h-6 w-6 text-red-600 flex-shrink-0 mt-0.5" />
                  <div>
                    <h3 className="text-red-900 font-bold text-base mb-2">Error</h3>
                    <p className="text-red-700 text-sm">{error}</p>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Results Section */}
          {results && !isLoading && (
            <div className="space-y-6">
              {apiMode === 'agent' && results.mode === 'base_case' && results.note && (
                <div className="max-w-4xl mx-auto">
                  <div className="bg-amber-50 backdrop-blur-md border-2 border-amber-200 rounded-2xl p-6 shadow-xl">
                    <div className="flex items-start space-x-3">
                      <Sparkles className="h-6 w-6 text-amber-600 mt-0.5 flex-shrink-0" />
                      <div className="flex-1">
                        <h3 className="text-amber-900 font-bold text-base mb-2">
                          Fallback to Semantic Search
                        </h3>
                        <p className="text-amber-800 text-sm mb-2">{results.note}</p>
                        {results.interpreted_query && (
                          <div className="bg-white/60 rounded-lg p-3 border border-amber-200 mt-2">
                            <p className="text-sm text-amber-700">
                              <span className="font-semibold">Interpreted Query:</span> "{results.interpreted_query}"
                            </p>
                          </div>
                        )}
                      </div>
                    </div>
                  </div>
                </div>
              )}

              {results.mode === 'special_case' ? (
                <div className="space-y-8">
                  <SpecialCase data={results} query={query} />

                  <Interpreter results={results} query={query} />

                  {results.baseCase && (
                    <div className="mt-8">
                      <BaseCase data={results.baseCase} query={results.semantic_query_used || query} />
                    </div>
                  )}
                </div>
              ) : (
                <>
                  <Interpreter results={results} query={query} />
                  <BaseCase data={results} query={results.interpreted_query || query} />
                </>
              )}
            </div>
          )}
        </div>
      </div>

      <Footer />

      {/* Help Modal always available */}
      <AgentHelp open={showHelp} onClose={() => setShowHelp(false)} />
    </div>
  );
};

export default Home;