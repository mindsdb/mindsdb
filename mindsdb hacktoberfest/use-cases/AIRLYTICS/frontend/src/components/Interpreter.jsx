// src/components/Interpreter.jsx
import { useState } from 'react';
import { Brain, Lightbulb, Loader2, Sparkles, AlertCircle, ChevronDown, ChevronUp, Zap, TrendingUp, Target, ArrowRight } from 'lucide-react';

const Interpreter = ({ results, query }) => {
  const [isInterpreting, setIsInterpreting] = useState(false);
  const [interpretError, setInterpretError] = useState(null);
  const [agentText, setAgentText] = useState(null);
  const [isExpanded, setIsExpanded] = useState(false);

  const handleInterpret = async () => {
    if (!results) return;

    try {
      setIsExpanded(true);
      setIsInterpreting(true);
      setInterpretError(null);
      setAgentText(null);

      const mode = results.mode || 'base_case';
      const topRows = (results.display_rows || []).slice(0, 5);

      let reintr_query = '';
      if (mode === 'special_case') reintr_query = results.semantic_query_used || '';
      else reintr_query = results.interpreted_query || '';

      let baseStats = null;
      if (mode === 'special_case') baseStats = results.base_stats || {};
      else baseStats = results.summary_stats || {};

      let specialStats = null;
      if (mode === 'special_case') {
        specialStats = results.multivalue_stats || null;
        if (specialStats && results.user_message) {
          specialStats = {
            ...specialStats,
            description: results.user_message
          };
        }
      }

      const payload = {
        query: query || '',
        reintr_query: reintr_query || '',
        top_reviews: topRows.map(r => ({
          airline_name: r.airline_name || 'Unknown',
          overall_rating: r.overall_rating,
          recommended: r.recommended,
          verified: r.verified,
          seat_type: r.seat_type,
          type_of_traveller: r.type_of_traveller,
          review: r.review || ''
        })),
        base_stats: baseStats || {},
        special_stats: specialStats
      };

      console.log('📤 Sending to interpret agent:', {
        mode,
        has_reintr_query: !!payload.reintr_query,
        has_base_stats: Object.keys(payload.base_stats).length > 0,
        has_special_stats: !!payload.special_stats,
        has_description: specialStats?.description ? true : false,
        top_reviews_count: payload.top_reviews.length
      });

      const resp = await fetch('http://127.0.0.1:8000/interpret_agent', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      if (!resp.ok) {
        const errorText = await resp.text();
        throw new Error(`API Error: ${resp.status} - ${errorText}`);
      }

      const data = await resp.json();
      setAgentText(data.answer || 'No interpretation returned.');

    } catch (err) {
      console.error('❌ Interpret failed:', err);
      setInterpretError(err.message || 'Failed to interpret results.');
    } finally {
      setIsInterpreting(false);
    }
  };

  return (
    <div className="max-w-6xl mx-auto">
      <div className="bg-white/95 backdrop-blur-xl border-2 border-gray-200 rounded-2xl shadow-xl overflow-hidden">
        
        {/* Header */}
        <div className="relative bg-gradient-to-r from-emerald-50 via-teal-50 to-emerald-50 border-b-2 border-emerald-200 p-6">
          <div className="relative flex items-center justify-between">
            <div className="flex items-center space-x-4">
              <div className="relative">
                <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-emerald-600 to-teal-600 flex items-center justify-center shadow-lg">
                  <Lightbulb className="h-6 w-6 text-white" />
                </div>
              </div>
              <div>
                <div className="flex items-center gap-2 mb-1">
                  <h3 className="text-xl font-bold text-gray-900">InsightInterpreter</h3>
                  <div className="inline-flex items-center gap-1 bg-gradient-to-r from-emerald-600 to-teal-600 text-white text-xs font-bold px-2.5 py-1 rounded-full shadow-md">
                    <Sparkles className="h-3 w-3" />
                    AI
                  </div>
                </div>
                <p className="text-sm text-gray-600 font-medium">Actionable intelligence from your data, delivered instantly</p>
              </div>
            </div>

            {/* Toggle Button */}
            <button
              onClick={() => setIsExpanded(!isExpanded)}
              className="group p-3 rounded-xl hover:bg-white/70 transition-colors duration-200 border-2 border-transparent hover:border-emerald-300"
            >
              {isExpanded ? (
                <ChevronUp className="h-6 w-6 text-gray-700 group-hover:text-emerald-600 transition-colors" />
              ) : (
                <ChevronDown className="h-6 w-6 text-gray-700 group-hover:text-emerald-600 transition-colors" />
              )}
            </button>
          </div>
        </div>

        {/* Collapsible Content */}
        {isExpanded && (
          <div className="p-8">

            {/* Default CTA */}
            {!agentText && !isInterpreting && !interpretError && (
              <div className="text-center py-10">
                <div className="mb-6">
                  <div className="relative inline-block mb-6">
                    <Brain className="h-16 w-16 text-emerald-600 mx-auto" />
                  </div>
                  
                  <h4 className="text-2xl font-bold text-gray-900 mb-3">
                    Ready to Decode Your Data?
                  </h4>
                  
                  <p className="text-gray-600 text-base mb-8 max-w-2xl mx-auto leading-relaxed">
                    InsightInterpreter analyzes your results with the <span className="font-bold text-gray-900">precision of a veteran airline analyst</span>. 
                    No corporate fluff. No obvious recaps. Just <span className="font-bold text-emerald-700">sharp, actionable insights</span> that tell you 
                    <span className="font-bold text-emerald-700"> exactly what to fix and why</span>.
                  </p>

                  {/* Feature Pills */}
                  <div className="flex flex-wrap items-center justify-center gap-3 mb-8">
                    <div className="inline-flex items-center gap-2 bg-blue-50 border border-blue-200 rounded-full px-4 py-2.5">
                      <TrendingUp className="h-4 w-4 text-blue-600" />
                      <span className="text-sm font-bold text-blue-700">Finds Hidden Patterns</span>
                    </div>
                    <div className="inline-flex items-center gap-2 bg-purple-50 border border-purple-200 rounded-full px-4 py-2.5">
                      <Zap className="h-4 w-4 text-purple-600" />
                      <span className="text-sm font-bold text-purple-700">Reveals Root Causes</span>
                    </div>
                    <div className="inline-flex items-center gap-2 bg-emerald-50 border border-emerald-200 rounded-full px-4 py-2.5">
                      <Target className="h-4 w-4 text-emerald-600" />
                      <span className="text-sm font-bold text-emerald-700">Suggests Actions</span>
                    </div>
                  </div>
                </div>

                <button
                  onClick={handleInterpret}
                  className="group inline-flex items-center gap-3 px-10 py-5 rounded-xl font-bold text-lg text-white bg-gradient-to-r from-emerald-600 to-teal-600 hover:from-emerald-700 hover:to-teal-700 shadow-lg hover:shadow-xl transition-all duration-200"
                >
                  <Sparkles className="h-5 w-5" />
                  <span>Unlock AI Insights</span>
                  <ArrowRight className="h-5 w-5 group-hover:translate-x-1 transition-transform" />
                </button>

                <p className="text-xs text-gray-500 mt-6 font-medium">
                  Analyzes top-matched reviews and statistical patterns from your query
                </p>
              </div>
            )}

            {/* Loading State */}
            {isInterpreting && (
              <div className="py-16 text-center">
                <div className="inline-flex flex-col items-center space-y-6">
                  <div className="relative">
                    <div className="w-20 h-20 rounded-full bg-gradient-to-br from-emerald-600 to-teal-600 flex items-center justify-center shadow-xl">
                      <Brain className="w-10 h-10 text-white animate-pulse" />
                    </div>
                  </div>
                  
                  <div className="space-y-2">
                    <span className="text-xl font-bold text-gray-900 block">
                      Analyzing Your Results...
                    </span>
                    <span className="text-sm text-gray-600 block max-w-md font-medium">
                      InsightInterpreter is processing patterns, correlations, and hidden signals in your data
                    </span>
                  </div>

                  {/* Loading indicator */}
                  <div className="flex gap-2">
                    <div className="w-2.5 h-2.5 rounded-full bg-emerald-600 animate-pulse"></div>
                    <div className="w-2.5 h-2.5 rounded-full bg-teal-600 animate-pulse" style={{ animationDelay: '0.2s' }}></div>
                    <div className="w-2.5 h-2.5 rounded-full bg-emerald-600 animate-pulse" style={{ animationDelay: '0.4s' }}></div>
                  </div>
                </div>
              </div>
            )}

            {/* Error State */}
            {interpretError && !isInterpreting && (
              <div className="bg-red-50 border-2 border-red-200 rounded-xl p-6 shadow-lg">
                <div className="flex items-start space-x-4">
                  <div className="w-10 h-10 rounded-lg bg-red-600 flex items-center justify-center flex-shrink-0">
                    <AlertCircle className="h-5 w-5 text-white" />
                  </div>
                  <div className="flex-1">
                    <h4 className="text-red-900 font-bold text-base mb-2">Failed to Generate Insights</h4>
                    <p className="text-red-700 text-sm mb-4 leading-relaxed">{interpretError}</p>
                    <button
                      onClick={handleInterpret}
                      className="inline-flex items-center gap-2 px-5 py-2.5 rounded-lg font-bold text-sm text-white bg-red-600 hover:bg-red-700 transition-colors duration-200"
                    >
                      <Brain className="h-4 w-4" />
                      <span>Try Again</span>
                    </button>
                  </div>
                </div>
              </div>
            )}

            {/* Success State */}
            {agentText && !isInterpreting && (
              <div className="space-y-6">
                {/* Insight Box */}
                <div className="bg-gradient-to-br from-emerald-50 via-teal-50 to-emerald-50 border-2 border-emerald-300 rounded-xl p-7 shadow-lg">
                  {/* Header */}
                  <div className="flex items-center space-x-3 mb-5">
                    <div className="w-11 h-11 rounded-lg bg-gradient-to-br from-emerald-600 to-teal-600 flex items-center justify-center shadow-md">
                      <Lightbulb className="h-6 w-6 text-white" />
                    </div>
                    <div>
                      <h4 className="font-bold text-emerald-900 text-lg">AI Analysis & Recommendations</h4>
                      <p className="text-xs text-emerald-700 font-medium">Based on your query results and statistical patterns</p>
                    </div>
                  </div>

                  {/* Content */}
                  <div className="bg-white rounded-lg p-6 border border-emerald-200 shadow-sm">
                    <div className="prose prose-sm max-w-none">
                      <p className="whitespace-pre-wrap text-gray-800 leading-relaxed text-base font-medium">
                        {agentText}
                      </p>
                    </div>
                  </div>

                  {/* Footer tag */}
                  <div className="mt-5 flex items-center justify-between">
                    <div className="inline-flex items-center gap-2 bg-emerald-100 border border-emerald-300 rounded-full px-4 py-2">
                      <Sparkles className="h-4 w-4 text-emerald-600" />
                      <span className="text-xs font-bold text-emerald-700">Powered by InsightInterpreter AI</span>
                    </div>
                  </div>
                </div>

                {/* Action Button */}
                <div className="flex justify-end">
                  <button
                    onClick={handleInterpret}
                    disabled={isInterpreting}
                    className="inline-flex items-center gap-2 px-6 py-3 rounded-lg font-bold text-sm text-emerald-700 bg-emerald-100 hover:bg-emerald-200 border border-emerald-200 transition-colors duration-200 disabled:opacity-50"
                  >
                    <Sparkles className="h-4 w-4" />
                    <span>Regenerate Insights</span>
                  </button>
                </div>
              </div>
            )}

          </div>
        )}
      </div>
    </div>
  );
};

export default Interpreter;