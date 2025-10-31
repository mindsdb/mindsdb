import {
  X,
  HelpCircle,
  BookOpen,
  Sliders,
  FunctionSquare,
  Info,
  Sparkles,
  ListChecks,
  BarChart3,
  PieChart,
  Brain,
  Search,
  CheckCircle2,
  ArrowRight,
  Lightbulb
} from 'lucide-react';

const FIELD_SCHEMA = {
  numeric: [
    { key: 'overall_rating', range: '1 to 10' },
    { key: 'seat_comfort', range: '1 to 5' },
    { key: 'cabin_staff_service', range: '1 to 5' },
    { key: 'food_beverages', range: '1 to 5' },
    { key: 'ground_service', range: '1 to 5' },
    { key: 'inflight_entertainment', range: '1 to 5' },
    { key: 'wifi_connectivity', range: '1 to 5' },
    { key: 'value_for_money', range: '1 to 5' }
  ],
  categorical: [
    { key: 'recommended', values: ['yes', 'no'] },
    { key: 'verified', values: ['true', 'false'] },
    { key: 'seat_type', values: ['Economy Class', 'Business Class', 'Premium Economy', 'First Class'] },
    { key: 'type_of_traveller', values: ['Solo Leisure', 'Couple Leisure', 'Family Leisure', 'Business'] },
    { key: 'airline_name', values: ['Top airlines or Others'] }
  ]
};

const SEMANTIC_EXAMPLES = [
  'excellent legroom and comfortable seats',
  'best crew and inflight service',
  'very long delay and bad food',
  'lost baggage, delayed luggage, mishandled bag',
  'value for money, affordable tickets',
];

const ANALYTICS_EXAMPLES = [
  {
    title: 'Filtered distribution',
    text:
      "For passengers complaining about check in delays, show seat type distribution among those who rated ground service above 3.",
    hint: 'Runs conditional_distribution_analysis'
  },
  {
    title: 'Category vs category',
    text:
      "Among reviews mentioning crowded flights, compare how many Economy Class passengers were Solo Leisure travelers versus Business travelers.",
    hint: 'Runs conditional_category_to_category_analysis'
  },
  {
    title: 'Rating vs rating',
    text:
      "Users who complained about baggage claim delays, what percent of those who rated ground service above 4 also had overall rating below 5?",
    hint: 'Runs conditional_rating_to_rating_analysis'
  },
  {
    title: 'Rating to outcome',
    text:
      "Among users who mentioned poor inflight meals, what percent of those who rated food and beverages above 4 recommended the airline?",
    hint: 'Runs conditional_rating_analysis'
  },
  {
    title: 'Natural language retrieval only',
    text:
      "Show me the reviews of the users who had best flight experience.",
    hint: 'Agent reinterprets as semantic retrieval if no stats are requested'
  }
];

const AgentHelp = ({ open, onClose }) => {
  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 overflow-y-auto">
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black/50 backdrop-blur-sm"
        onClick={onClose}
      />

      {/* Modal */}
      <div className="relative flex items-start justify-center min-h-screen py-8 px-4">
        <div className="relative w-full max-w-6xl bg-gradient-to-br from-white via-blue-50/30 to-purple-50/30 rounded-3xl shadow-2xl border border-gray-200 overflow-hidden">
          {/* Header */}
          <div className="sticky top-0 z-10 bg-gradient-to-r from-blue-600 via-indigo-600 to-purple-600 text-white">
            <div className="flex items-center justify-between px-8 py-6">
              <div className="flex items-center gap-4">
                <div className="w-14 h-14 rounded-2xl bg-white/20 backdrop-blur-sm flex items-center justify-center ring-2 ring-white/40 shadow-lg">
                  <HelpCircle className="h-7 w-7" />
                </div>
                <div>
                  <h2 className="text-2xl font-bold tracking-tight">Intelligent Search & Analytics Guide</h2>
                  <p className="text-sm text-blue-100 mt-1">
                    Two powerful modes. One seamless experience. Choose what fits your needs.
                  </p>
                </div>
              </div>
              <button
                onClick={onClose}
                className="p-2.5 rounded-xl hover:bg-white/20 transition-all duration-200"
                aria-label="Close"
              >
                <X className="h-6 w-6" />
              </button>
            </div>
          </div>

          {/* Body */}
          <div className="px-8 py-8 space-y-12">
            {/* Section A: Semantic Search (Standard Mode) */}
            <section className="bg-white rounded-2xl p-8 shadow-md border border-blue-100">
              <div className="flex items-center gap-3 mb-6">
                <div className="bg-gradient-to-br from-blue-500 to-blue-600 w-12 h-12 rounded-xl flex items-center justify-center text-white shadow-lg">
                  <Search className="h-6 w-6" />
                </div>
                <div>
                  <h3 className="text-2xl font-bold text-gray-900">Semantic Search</h3>
                  <p className="text-sm text-blue-600 font-medium">Standard Mode</p>
                </div>
              </div>

              <p className="text-gray-700 leading-relaxed mb-6">
                Perfect for finding the most relevant reviews matching your keywords or phrases. 
                Uses advanced embedding-based retrieval with intelligent keyword biasing to surface 
                the best matches and provide comprehensive statistical summaries.
              </p>

              <div className="grid lg:grid-cols-2 gap-6">
                {/* Examples */}
                <div className="bg-gradient-to-br from-blue-50 to-indigo-50 rounded-xl border-2 border-blue-200 p-6">
                  <div className="flex items-center gap-2 mb-4">
                    <Sparkles className="h-5 w-5 text-blue-600" />
                    <p className="font-bold text-gray-900">Try These Queries</p>
                  </div>
                  <ul className="space-y-3">
                    {SEMANTIC_EXAMPLES.map((t, i) => (
                      <li key={i} className="flex items-start gap-3 group">
                        <div className="mt-1.5 w-6 h-6 rounded-lg bg-blue-500 flex items-center justify-center flex-shrink-0 text-white text-xs font-bold">
                          {i + 1}
                        </div>
                        <span className="text-gray-800 font-medium">"{t}"</span>
                      </li>
                    ))}
                  </ul>
                </div>

                {/* What You Get */}
                <div className="bg-white rounded-xl border-2 border-gray-200 p-6">
                  <div className="flex items-center gap-2 mb-4">
                    <CheckCircle2 className="h-5 w-5 text-green-600" />
                    <p className="font-bold text-gray-900">Comprehensive Results</p>
                  </div>
                  <div className="grid gap-4">
                    <div className="flex items-start gap-3 p-3 bg-gray-50 rounded-lg border border-gray-200">
                      <BarChart3 className="h-5 w-5 text-blue-600 mt-0.5 flex-shrink-0" />
                      <div>
                        <p className="font-semibold text-gray-900 text-sm">Summary Statistics</p>
                        <p className="text-xs text-gray-600 mt-0.5">Average rating, total reviews, recommend rate, seat comfort</p>
                      </div>
                    </div>
                    <div className="flex items-start gap-3 p-3 bg-gray-50 rounded-lg border border-gray-200">
                      <PieChart className="h-5 w-5 text-indigo-600 mt-0.5 flex-shrink-0" />
                      <div>
                        <p className="font-semibold text-gray-900 text-sm">Distribution Analysis</p>
                        <p className="text-xs text-gray-600 mt-0.5">Seat type and traveler type breakdowns</p>
                      </div>
                    </div>
                    <div className="flex items-start gap-3 p-3 bg-gray-50 rounded-lg border border-gray-200">
                      <ListChecks className="h-5 w-5 text-green-600 mt-0.5 flex-shrink-0" />
                      <div>
                        <p className="font-semibold text-gray-900 text-sm">Detailed Ratings</p>
                        <p className="text-xs text-gray-600 mt-0.5">Seat comfort, staff, food, ground service, entertainment, wifi, value</p>
                      </div>
                    </div>
                    <div className="flex items-start gap-3 p-3 bg-gray-50 rounded-lg border border-gray-200">
                      <Brain className="h-5 w-5 text-purple-600 mt-0.5 flex-shrink-0" />
                      <div>
                        <p className="font-semibold text-gray-900 text-sm">Advanced Statistics</p>
                        <p className="text-xs text-gray-600 mt-0.5">Correlations, field completeness, per category averages</p>
                      </div>
                    </div>
                  </div>
                  <div className="mt-4 pt-4 border-t border-gray-200">
                    <p className="text-xs text-gray-600 leading-relaxed">
                      <span className="font-semibold text-gray-900">Plus:</span> Top 50 matched reviews with full metadata including traveler type, seat class, per-metric ratings, and recommendation labels.
                    </p>
                  </div>
                </div>
              </div>
            </section>

            {/* Section B: Agent Analytics (Advanced Mode) */}
            <section className="bg-gradient-to-br from-purple-50 via-indigo-50 to-purple-50 rounded-2xl p-8 shadow-lg border-2 border-purple-200">
              <div className="flex items-center gap-3 mb-6">
                <div className="bg-gradient-to-br from-purple-600 to-indigo-600 w-12 h-12 rounded-xl flex items-center justify-center text-white shadow-lg">
                  <Brain className="h-6 w-6" />
                </div>
                <div>
                  <div className="flex items-center gap-2 mb-1">
                    <h3 className="text-2xl font-bold text-gray-900">Agent Analytics</h3>
                    <div className="inline-flex items-center gap-1 bg-gradient-to-r from-purple-600 to-indigo-600 text-white text-xs font-bold px-3 py-1 rounded-full shadow-md">
                      <Sparkles className="h-3 w-3" />
                      ADVANCED
                    </div>
                  </div>
                  <p className="text-purple-700 font-medium">Intelligent Query Interpretation with Multi-Dimensional Analytics</p>
                </div>
              </div>

              {/* Natural Language Power */}
              <div className="bg-white rounded-xl p-6 mb-6 shadow-sm border border-purple-100">
                <div className="flex items-start gap-4">
                  <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-purple-500 to-indigo-500 text-white flex items-center justify-center flex-shrink-0 shadow-md">
                    <Sparkles className="h-5 w-5" />
                  </div>
                  <div>
                    <h4 className="text-lg font-bold text-gray-900 mb-2">Talk Like a Human, Get Expert Analysis</h4>
                    <p className="text-gray-700 leading-relaxed mb-3">
                      Describe your question naturally. Mention groups, contexts, thresholds, or comparisons. 
                      The agent understands your intent, maps it to structured fields, and automatically chooses 
                      between semantic retrieval or advanced statistical analysis.
                    </p>
                    <div className="flex flex-wrap gap-2">
                      <span className="inline-flex items-center gap-1 text-xs font-semibold bg-purple-100 text-purple-700 px-3 py-1 rounded-full">
                        <CheckCircle2 className="h-3 w-3" />
                        Zero training required
                      </span>
                      <span className="inline-flex items-center gap-1 text-xs font-semibold bg-indigo-100 text-indigo-700 px-3 py-1 rounded-full">
                        <CheckCircle2 className="h-3 w-3" />
                        Automatic function selection
                      </span>
                      <span className="inline-flex items-center gap-1 text-xs font-semibold bg-purple-100 text-purple-700 px-3 py-1 rounded-full">
                        <CheckCircle2 className="h-3 w-3" />
                        10,000+ analytical paths
                      </span>
                    </div>
                  </div>
                </div>
              </div>

              <p className="text-gray-800 font-medium leading-relaxed mb-6 text-center">
                The agent intelligently reinterprets your question, generates optimized semantic queries, 
                selects the right analytical function, and delivers <span className="font-bold text-purple-700">everything from Standard Mode plus advanced charts and multi-value statistics</span>.
              </p>

              {/* Available Fields */}
              <div className="bg-white rounded-xl border border-purple-200 p-6 mb-6 shadow-sm">
                <div className="flex items-center gap-3 mb-5">
                  <BookOpen className="h-5 w-5 text-purple-700" />
                  <h4 className="text-lg font-bold text-gray-900">Available Data Fields</h4>
                </div>
                <div className="grid md:grid-cols-2 gap-4">
                  {/* Numeric Fields */}
                  <div className="bg-gradient-to-br from-blue-50 to-indigo-50 rounded-xl p-4 border border-blue-200">
                    <p className="text-xs font-bold text-blue-700 uppercase tracking-wide mb-3 flex items-center gap-2">
                      <Sliders className="h-4 w-4" />
                      Numeric Ratings
                    </p>
                    <ul className="space-y-2">
                      {FIELD_SCHEMA.numeric.map(item => (
                        <li key={item.key} className="flex items-center justify-between text-sm bg-white rounded-lg px-3 py-2 shadow-sm">
                          <span className="text-gray-800 font-medium">{item.key.replace(/_/g, ' ')}</span>
                          <span className="text-blue-600 text-xs font-bold bg-blue-100 px-2 py-1 rounded">{item.range}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                  
                  {/* Categorical Fields */}
                  <div className="bg-gradient-to-br from-purple-50 to-pink-50 rounded-xl p-4 border border-purple-200">
                    <p className="text-xs font-bold text-purple-700 uppercase tracking-wide mb-3 flex items-center gap-2">
                      <FunctionSquare className="h-4 w-4" />
                      Categories & Flags
                    </p>
                    <ul className="space-y-2">
                      {FIELD_SCHEMA.categorical.map(item => (
                        <li key={item.key} className="bg-white rounded-lg px-3 py-2 shadow-sm">
                          <div className="flex items-start justify-between gap-2 text-sm">
                            <span className="text-gray-800 font-medium">{item.key.replace(/_/g, ' ')}</span>
                            <span className="text-purple-600 text-xs text-right font-medium">
                              {Array.isArray(item.values) ? item.values.join(', ') : 'true/false'}
                            </span>
                          </div>
                        </li>
                      ))}
                    </ul>
                  </div>
                </div>
                <p className="text-sm text-gray-600 mt-4 text-center italic">
                  Drop these field names in your queries to unlock precise filtering and multi-dimensional analysis
                </p>
              </div>

              {/* Analytical Functions */}
              <div className="bg-white rounded-xl border border-indigo-200 p-6 mb-6 shadow-sm">
                <div className="flex items-center gap-3 mb-5">
                  <FunctionSquare className="h-5 w-5 text-indigo-700" />
                  <h4 className="text-lg font-bold text-gray-900">Five Intelligent Analytical Functions</h4>
                </div>
                <div className="grid gap-3">
                  {[
                    { num: '01', name: 'conditional_rating_analysis', desc: 'Numeric rating → categorical/boolean outcome. Perfect for "who recommends when metric X exceeds threshold Y"' },
                    { num: '02', name: 'conditional_rating_to_rating_analysis', desc: 'Numeric ↔ numeric with thresholds. Ideal for "low wifi + low satisfaction" intersections' },
                    { num: '03', name: 'conditional_category_to_category_analysis', desc: 'Category ↔ category breakdowns. Compare seat classes vs traveler types and similar pivots' },
                    { num: '04', name: 'conditional_distribution_analysis', desc: 'Filter first, then show distribution. "Among X group, show breakdown of Y category"' },
                    { num: '05', name: 'general_percentage_distribution', desc: 'Single numeric field percentages for rapid threshold checks across rating scales' }
                  ].map((fn) => (
                    <div key={fn.num} className="flex items-start gap-4 p-4 bg-gradient-to-r from-indigo-50 to-purple-50 rounded-xl border border-indigo-200">
                      <div className="w-10 h-10 rounded-lg bg-gradient-to-br from-indigo-600 to-purple-600 text-white flex items-center justify-center font-bold flex-shrink-0 shadow-md">
                        {fn.num}
                      </div>
                      <div>
                        <p className="font-bold text-indigo-700 mb-1">{fn.name}</p>
                        <p className="text-sm text-gray-700">{fn.desc}</p>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              {/* Example Queries */}
              <div className="bg-white rounded-xl border border-pink-200 p-6 shadow-sm">
                <div className="flex items-center gap-3 mb-5">
                  <Sparkles className="h-5 w-5 text-pink-600" />
                  <h4 className="text-lg font-bold text-gray-900">Try These Advanced Queries</h4>
                </div>
                <div className="grid gap-4">
                  {ANALYTICS_EXAMPLES.map((ex, i) => (
                    <div key={i} className="p-5 bg-gradient-to-br from-white to-pink-50 rounded-xl border border-pink-200">
                      <div className="flex items-start gap-4">
                        <div className="w-10 h-10 rounded-lg bg-gradient-to-br from-pink-500 to-rose-500 text-white flex items-center justify-center font-bold flex-shrink-0 shadow-md">
                          {i + 1}
                        </div>
                        <div className="flex-1">
                          <div className="flex items-center gap-2 mb-2">
                            <span className="text-xs font-bold text-pink-700 bg-pink-100 px-3 py-1 rounded-full uppercase tracking-wide">
                              {ex.title}
                            </span>
                          </div>
                          <p className="text-gray-800 font-medium mb-2 leading-relaxed">"{ex.text}"</p>
                          <div className="flex items-center gap-2 text-xs text-gray-600">
                            <Info className="h-3.5 w-3.5 text-indigo-500" />
                            <span className="italic">{ex.hint}</span>
                          </div>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
                <div className="mt-5 p-4 bg-gradient-to-r from-purple-100 to-pink-100 rounded-xl border border-purple-200">
                  <p className="text-sm text-gray-700 text-center">
                    <span className="font-bold text-purple-700">Smart routing:</span> Statistical queries trigger functions automatically. 
                    Pure retrieval queries get optimized for sharper semantic matching with full standard summaries.
                  </p>
                </div>
              </div>
            </section>

            {/* AI Insights Section */}
<section className="bg-gradient-to-br from-emerald-50 via-teal-50 to-emerald-50 rounded-2xl p-8 shadow-lg border-2 border-emerald-200">
  <div className="flex items-center gap-3 mb-6">
    <div className="bg-gradient-to-br from-emerald-500 to-teal-600 w-12 h-12 rounded-xl flex items-center justify-center text-white shadow-lg">
      <Lightbulb className="h-6 w-6" />
    </div>
    <div>
      <h3 className="text-2xl font-bold text-gray-900">AI Insights & Interpretation</h3>
      <p className="text-sm text-emerald-700 font-medium">Available for Both Modes</p>
    </div>
  </div>

  <p className="text-gray-700 leading-relaxed mb-6">
    After running any query, you can activate <span className="font-bold text-emerald-700">InsightInterpreter</span> — 
    your AI data strategist that cuts through noise, spots patterns, and tells you what truly matters.
  </p>

  <div className="bg-white rounded-xl border border-emerald-200 p-6 mb-6 shadow-sm">
    <div className="flex items-start gap-4">
      <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-emerald-500 to-teal-600 text-white flex items-center justify-center flex-shrink-0 shadow-md">
        <Brain className="h-5 w-5" />
      </div>
      <div>
        <h4 className="text-lg font-bold text-gray-900 mb-2">What InsightInterpreter Does</h4>
        <p className="text-gray-700 leading-relaxed mb-4">
          Think of it as your airline’s in-house AI consultant. It doesn’t just summarize, it interprets. 
          From distributions and correlations to customer sentiment, it explains *why* things happen and *what to do next*.
        </p>
        <div className="space-y-2">
          <div className="flex items-start gap-2">
            <CheckCircle2 className="h-4 w-4 text-emerald-600 mt-0.5 flex-shrink-0" />
            <p className="text-sm text-gray-700"><span className="font-semibold">Spots contradictions</span> — like high ratings but low loyalty</p>
          </div>
          <div className="flex items-start gap-2">
            <CheckCircle2 className="h-4 w-4 text-emerald-600 mt-0.5 flex-shrink-0" />
            <p className="text-sm text-gray-700"><span className="font-semibold">Reveals true drivers</span> — what’s actually shaping satisfaction</p>
          </div>
          <div className="flex items-start gap-2">
            <CheckCircle2 className="h-4 w-4 text-emerald-600 mt-0.5 flex-shrink-0" />
            <p className="text-sm text-gray-700"><span className="font-semibold">Suggests next moves</span> — from service tweaks to strategic fixes</p>
          </div>
          <div className="flex items-start gap-2">
            <CheckCircle2 className="h-4 w-4 text-emerald-600 mt-0.5 flex-shrink-0" />
            <p className="text-sm text-gray-700"><span className="font-semibold">Ends with clarity</span> — gives you the “what now” moment instantly</p>
          </div>
        </div>
      </div>
    </div>
  </div>

  <div className="grid md:grid-cols-2 gap-4 mb-6">
    <div className="bg-white rounded-xl border border-emerald-200 p-5 shadow-sm">
      <div className="flex items-center gap-2 mb-3">
        <div className="w-8 h-8 rounded-lg bg-red-100 flex items-center justify-center">
          <X className="h-4 w-4 text-red-600" />
        </div>
        <p className="font-bold text-gray-900 text-sm">What It Doesn't Do</p>
      </div>
      <ul className="space-y-2 text-sm text-gray-700">
        <li className="flex items-start gap-2">
          <span className="text-red-500 font-bold">×</span>
          <span>Repeat your query or list basic stats</span>
        </li>
        <li className="flex items-start gap-2">
          <span className="text-red-500 font-bold">×</span>
          <span>Use filler like “The data shows...”</span>
        </li>
        <li className="flex items-start gap-2">
          <span className="text-red-500 font-bold">×</span>
          <span>Give shallow or generic takeaways</span>
        </li>
      </ul>
    </div>

    <div className="bg-white rounded-xl border border-emerald-200 p-5 shadow-sm">
      <div className="flex items-center gap-2 mb-3">
        <div className="w-8 h-8 rounded-lg bg-emerald-100 flex items-center justify-center">
          <CheckCircle2 className="h-4 w-4 text-emerald-600" />
        </div>
        <p className="font-bold text-gray-900 text-sm">What It Does</p>
      </div>
      <ul className="space-y-2 text-sm text-gray-700">
        <li className="flex items-start gap-2">
          <span className="text-emerald-600 font-bold">✓</span>
          <span>Uncover hidden cause-effect relationships</span>
        </li>
        <li className="flex items-start gap-2">
          <span className="text-emerald-600 font-bold">✓</span>
          <span>Speak in the voice of a data-driven strategist</span>
        </li>
        <li className="flex items-start gap-2">
          <span className="text-emerald-600 font-bold">✓</span>
          <span>Deliver precise, actionable next steps</span>
        </li>
      </ul>
    </div>
  </div>

  <div className="bg-gradient-to-r from-emerald-100 to-teal-100 rounded-xl border border-emerald-300 p-5">
    <p className="text-sm text-gray-800 leading-relaxed">
      <span className="font-bold text-emerald-800">Context matters:</span> InsightInterpreter works on the 
      <span className="font-semibold"> top N semantically matched reviews</span> (usually 100), interpreting them as your representative sample. 
      It highlights <span className="font-semibold">key levers</span> — Wi-Fi, staff, comfort, or pricing — helping management focus where it counts.
    </p>
  </div>

  <div className="mt-6 bg-white rounded-xl border border-emerald-200 p-5 shadow-sm">
    <div className="flex items-center gap-2 mb-3">
      <Info className="h-5 w-5 text-emerald-600" />
      <p className="font-bold text-gray-900">Example Insight Style</p>
    </div>
    <div className="space-y-3">
      <div className="p-3 bg-red-50 rounded-lg border border-red-200">
        <p className="text-xs font-semibold text-red-700 mb-1">❌ Generic</p>
        <p className="text-sm text-gray-700">"Passengers who rated Wi-Fi low also gave low overall ratings."</p>
      </div>
      <div className="p-3 bg-emerald-50 rounded-lg border border-emerald-200">
        <p className="text-xs font-semibold text-emerald-700 mb-1">✅ InsightInterpreter</p>
        <p className="text-sm text-gray-700 font-medium">"Wi-Fi below 3 stars drops total satisfaction by 68%. Upgrade connectivity, not catering."</p>
      </div>
    </div>
  </div>
</section>


            {/* Important Info Box */}
            <section className="bg-gradient-to-r from-blue-50 to-indigo-50 rounded-2xl border-2 border-blue-300 p-6 shadow-lg">
              <div className="flex items-start gap-4">
                <div className="w-12 h-12 rounded-xl bg-blue-600 flex items-center justify-center text-white flex-shrink-0 shadow-lg">
<Info className="h-6 w-6" />
</div>
<div>
<h4 className="font-bold text-gray-900 text-lg mb-2">How Analysis Works</h4>
<p className="text-gray-700 leading-relaxed">
By default, all calculations operate on the <span className="font-bold text-blue-700">top N semantically matched reviews</span> (typically N = 100)
based on your query or the agent's reinterpreted version. Feel free to mention specific airlines —
unknown names are automatically grouped as "Others" or matched to the closest relevant airline in the dataset.
</p>
</div>
</div>
</section>
</div>{/* Footer */}
      <div className="sticky bottom-0 bg-gradient-to-r from-gray-50 via-white to-gray-50 border-t-2 border-gray-200 px-8 py-5 flex items-center justify-between">
        <div className="flex items-center gap-2 text-sm text-gray-600">
          <CheckCircle2 className="h-4 w-4 text-green-600" />
          <span>Ready to explore intelligent analytics</span>
        </div>
        <button
          onClick={onClose}
          className="group px-6 py-3 rounded-xl bg-gradient-to-r from-blue-600 via-indigo-600 to-purple-600 text-white font-semibold shadow-lg hover:shadow-xl transform hover:scale-105 transition-all duration-300 flex items-center gap-2"
        >
          <span>Let's Get Started</span>
          <ArrowRight className="h-4 w-4 group-hover:translate-x-1 transition-transform" />
        </button>
      </div>
    </div>
  </div>
</div>);
};
export default AgentHelp;