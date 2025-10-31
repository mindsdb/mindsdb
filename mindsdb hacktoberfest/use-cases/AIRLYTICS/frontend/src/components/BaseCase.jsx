import { useState, useEffect } from 'react';
import { 
  Star, 
  Users, 
  TrendingUp, 
  ThumbsUp,
  ChevronRight,
  BarChart3,
  PieChart,
  CheckCircle,
  XCircle,
  ChevronDown,
  ChevronUp,
  Activity,
  Zap
} from 'lucide-react';

const BaseCase = ({ data, query }) => {
  const [visibleCount, setVisibleCount] = useState(5);
  const [animateStats, setAnimateStats] = useState(false);
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [showDistribution, setShowDistribution] = useState(false);
  const [showDetailedRatings, setShowDetailedRatings] = useState(false);
  const reviewsPerPage = 5;

  useEffect(() => {
    setAnimateStats(false);
    setTimeout(() => setAnimateStats(true), 100);
    setVisibleCount(5);
  }, [data]);

  if (!data) return null;

  const { summary_stats, display_rows, total_results_fetched } = data;
  const stats = summary_stats?.summary;

  const currentReviews = display_rows?.slice(0, visibleCount) || [];
  const hasMore = visibleCount < (display_rows?.length || 0);

  const loadMore = () => {
    if (hasMore) {
      const newCount = Math.min(visibleCount + reviewsPerPage, display_rows.length);
      setVisibleCount(newCount);
      
      setTimeout(() => {
        const reviewElements = document.querySelectorAll('[data-review-index]');
        const targetElement = reviewElements[visibleCount];
        if (targetElement) {
          targetElement.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
      }, 100);
    }
  };

  const keyMetrics = [
    {
      icon: <Star className="h-6 w-6" />,
      label: "Average Rating",
      value: stats?.numeric?.overall_rating?.mean?.toFixed(2) || "N/A",
      subtext: `out of ${stats?.numeric?.overall_rating?.max || 9}`,
      color: "blue"
    },
    {
      icon: <Users className="h-6 w-6" />,
      label: "Total Reviews",
      value: total_results_fetched?.toLocaleString() || "0",
      subtext: "analyzed",
      color: "indigo"
    },
    {
      icon: <ThumbsUp className="h-6 w-6" />,
      label: "Recommended",
      value: `${stats?.key_metrics?.recommended_yes_pct || 0}%`,
      subtext: "would fly again",
      color: "green"
    },
    {
      icon: <TrendingUp className="h-6 w-6" />,
      label: "Seat Comfort",
      value: stats?.numeric?.seat_comfort?.mean?.toFixed(1) || "N/A",
      subtext: `avg of ${stats?.numeric?.seat_comfort?.max || 5}`,
      color: "purple"
    }
  ];

  const seatTypeData = stats?.categorical?.seat_type?.distribution || {};
  const travelerTypeData = stats?.categorical?.type_of_traveller?.distribution || {};

  const getColorClass = (color) => {
    const colors = {
      blue: 'bg-blue-100 text-blue-600',
      indigo: 'bg-indigo-100 text-indigo-600',
      green: 'bg-green-100 text-green-600',
      purple: 'bg-purple-100 text-purple-600',
      red: 'bg-red-100 text-red-600',
      yellow: 'bg-yellow-100 text-yellow-600'
    };
    return colors[color] || colors.blue;
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 via-blue-50 to-indigo-50">
      <div className="max-w-7xl mx-auto px-4 py-8 space-y-8">
        
        {/* Query Info */}
        <div className="bg-blue-50 border border-blue-200 rounded-2xl p-4">
          <p className="text-blue-800">
            <span className="font-semibold">Query:</span> "{query}"
          </p>
          <p className="text-blue-600 text-sm mt-1">
            Found {total_results_fetched} matching reviews
          </p>
        </div>

        {/* Key Metrics Cards */}
        <div>
          <h2 className="text-2xl font-bold text-gray-900 mb-6 flex items-center">
            <BarChart3 className="h-6 w-6 mr-2 text-blue-600" />
            Summary Statistics
          </h2>
          <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-6">
            {keyMetrics.map((metric, index) => (
              <div
                key={index}
                className={`bg-white border border-gray-200 rounded-2xl p-6 hover:shadow-xl hover:border-blue-300 transition-all duration-300 ${
                  animateStats ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-4'
                }`}
                style={{ transitionDelay: `${index * 100}ms` }}
              >
                <div className={`${getColorClass(metric.color)} w-12 h-12 rounded-xl flex items-center justify-center mb-4`}>
                  {metric.icon}
                </div>
                <h3 className="text-3xl font-bold text-gray-900 mb-1">
                  {metric.value}
                </h3>
                <p className="text-sm text-gray-500 mb-1">{metric.label}</p>
                <p className="text-xs text-gray-400">{metric.subtext}</p>
              </div>
            ))}
          </div>
        </div>

        {/* Distribution Charts */}
        <div className="bg-white border border-gray-200 rounded-2xl overflow-hidden shadow-sm">
          <button
            onClick={() => setShowDistribution(!showDistribution)}
            className="w-full px-6 py-4 flex items-center justify-between hover:bg-gray-50 transition-colors"
          >
            <div className="flex items-center space-x-3">
              <div className="bg-gradient-to-br from-indigo-500 to-purple-500 w-10 h-10 rounded-lg flex items-center justify-center">
                <PieChart className="h-5 w-5 text-white" />
              </div>
              <div className="text-left">
                <h2 className="text-xl font-bold text-gray-900">Distribution Analysis</h2>
                <p className="text-sm text-gray-500">Seat type and traveler type breakdowns</p>
              </div>
            </div>
            {showDistribution ? (
              <ChevronUp className="h-6 w-6 text-gray-400" />
            ) : (
              <ChevronDown className="h-6 w-6 text-gray-400" />
            )}
          </button>

          {showDistribution && (
            <div className="px-6 pb-6 border-t border-gray-100 pt-6">
              <div className="grid md:grid-cols-2 gap-6">
            <div className="bg-white border border-gray-200 rounded-2xl p-6 hover:shadow-xl transition-all">
              <h3 className="font-semibold text-gray-900 mb-4">Seat Type Distribution</h3>
              <div className="space-y-3">
                {Object.entries(seatTypeData).map(([type, percentage], idx) => (
                  <div key={idx}>
                    <div className="flex justify-between text-sm mb-1">
                      <span className="text-gray-700">{type}</span>
                      <span className="text-gray-900 font-semibold">{percentage}%</span>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2">
                      <div
                        className="bg-gradient-to-r from-blue-500 to-indigo-500 h-2 rounded-full transition-all duration-1000"
                        style={{ 
                          width: `${percentage}%`,
                          transitionDelay: `${idx * 100}ms`
                        }}
                      />
                    </div>
                  </div>
                ))}
              </div>
            </div>

            <div className="bg-white border border-gray-200 rounded-2xl p-6 hover:shadow-xl transition-all">
              <h3 className="font-semibold text-gray-900 mb-4">Traveler Type Distribution</h3>
              <div className="space-y-3">
                {Object.entries(travelerTypeData).map(([type, percentage], idx) => (
                  <div key={idx}>
                    <div className="flex justify-between text-sm mb-1">
                      <span className="text-gray-700">{type}</span>
                      <span className="text-gray-900 font-semibold">{percentage}%</span>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2">
                      <div
                        className="bg-gradient-to-r from-purple-500 to-pink-500 h-2 rounded-full transition-all duration-1000"
                        style={{ 
                          width: `${percentage}%`,
                          transitionDelay: `${idx * 100}ms`
                        }}
                      />
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
            </div>
          )}
        </div>

        {/* Additional Numeric Stats */}
        <div className="bg-white border border-gray-200 rounded-2xl overflow-hidden shadow-sm">
          <button
            onClick={() => setShowDetailedRatings(!showDetailedRatings)}
            className="w-full px-6 py-4 flex items-center justify-between hover:bg-gray-50 transition-colors"
          >
            <div className="flex items-center space-x-3">
              <div className="bg-gradient-to-br from-blue-500 to-cyan-500 w-10 h-10 rounded-lg flex items-center justify-center">
                <BarChart3 className="h-5 w-5 text-white" />
              </div>
              <div className="text-left">
                <h2 className="text-xl font-bold text-gray-900">Detailed Ratings</h2>
                <p className="text-sm text-gray-500">Individual metric breakdowns</p>
              </div>
            </div>
            {showDetailedRatings ? (
              <ChevronUp className="h-6 w-6 text-gray-400" />
            ) : (
              <ChevronDown className="h-6 w-6 text-gray-400" />
            )}
          </button>

          {showDetailedRatings && (
            <div className="px-6 pb-6 border-t border-gray-100 pt-6">
              <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-4">
            {stats?.numeric && Object.entries(stats.numeric)
              .filter(([key]) => key !== 'overall_rating')
              .slice(0, 7)
              .map(([key, values], idx) => (
                <div
                  key={idx}
                  className="bg-white border border-gray-200 rounded-xl p-4 hover:shadow-lg transition-all"
                >
                  <p className="text-sm text-gray-600 mb-2 capitalize">
                    {key.replace(/_/g, ' ')}
                  </p>
                  <div className="flex items-end justify-between">
                    <span className="text-2xl font-bold text-gray-900">
                      {values.mean?.toFixed(1)}
                    </span>
                    <span className="text-sm text-gray-500">
                      / {values.max}
                    </span>
                  </div>
                  <div className="mt-2 w-full bg-gray-200 rounded-full h-1.5">
                    <div
                      className="bg-gradient-to-r from-green-400 to-blue-500 h-1.5 rounded-full transition-all duration-1000"
                      style={{ 
                        width: `${(values.mean / values.max) * 100}%`,
                        transitionDelay: `${idx * 50}ms`
                      }}
                    />
                  </div>
                </div>
              ))}
          </div>
            </div>
          )}
        </div>

        {/* Advanced Statistics */}
        <div className="bg-white border border-gray-200 rounded-2xl overflow-hidden shadow-sm">
          <button
            onClick={() => setShowAdvanced(!showAdvanced)}
            className="w-full px-6 py-4 flex items-center justify-between hover:bg-gray-50 transition-colors"
          >
            <div className="flex items-center space-x-3">
              <div className="bg-gradient-to-br from-purple-500 to-pink-500 w-10 h-10 rounded-lg flex items-center justify-center">
                <Activity className="h-5 w-5 text-white" />
              </div>
              <div className="text-left">
                <h2 className="text-xl font-bold text-gray-900">Advanced Statistics</h2>
                <p className="text-sm text-gray-500">Correlations, field completeness, and more insights</p>
              </div>
            </div>
            {showAdvanced ? (
              <ChevronUp className="h-6 w-6 text-gray-400" />
            ) : (
              <ChevronDown className="h-6 w-6 text-gray-400" />
            )}
          </button>

          {showAdvanced && (
            <div className="px-6 pb-6 space-y-6 border-t border-gray-100 pt-6">
              {/* Key Metrics */}
              <div>
                <h3 className="font-semibold text-gray-900 mb-4 flex items-center">
                  <Zap className="h-5 w-5 mr-2 text-yellow-500" />
                  Key Metrics
                </h3>
                <div className="grid md:grid-cols-3 gap-4">
                  <div className="bg-gradient-to-br from-green-50 to-emerald-50 rounded-xl p-4 border border-green-200">
                    <p className="text-sm text-green-600 mb-1">Positive Reviews</p>
                    <p className="text-2xl font-bold text-green-700">
                      {stats?.key_metrics?.positive_review_pct?.toFixed(1)}%
                    </p>
                  </div>
                  <div className="bg-gradient-to-br from-blue-50 to-indigo-50 rounded-xl p-4 border border-blue-200">
                    <p className="text-sm text-blue-600 mb-1">Verified Users</p>
                    <p className="text-2xl font-bold text-blue-700">
                      {stats?.key_metrics?.verified_user_pct?.toFixed(1)}%
                    </p>
                  </div>
                  <div className="bg-gradient-to-br from-purple-50 to-pink-50 rounded-xl p-4 border border-purple-200">
                    <p className="text-sm text-purple-600 mb-1">Would Recommend</p>
                    <p className="text-2xl font-bold text-purple-700">
                      {stats?.key_metrics?.recommended_yes_pct?.toFixed(1)}%
                    </p>
                  </div>
                </div>
              </div>

              {/* Average Ratings by Category */}
              {stats?.avg_rating_by_category?.type_of_traveller && (
                <div>
                  <h3 className="font-semibold text-gray-900 mb-4">Average Rating by Traveler Type</h3>
                  <div className="grid md:grid-cols-2 gap-4">
                    {Object.entries(stats.avg_rating_by_category.type_of_traveller).map(([type, rating]) => (
                      <div key={type} className="bg-gray-50 rounded-xl p-4 border border-gray-200">
                        <div className="flex items-center justify-between">
                          <span className="text-gray-700 font-medium">{type}</span>
                          <span className="text-2xl font-bold text-gray-900">{rating.toFixed(1)}</span>
                        </div>
                        <div className="mt-2 w-full bg-gray-200 rounded-full h-2">
                          <div
                            className="bg-gradient-to-r from-indigo-500 to-purple-500 h-2 rounded-full transition-all duration-500"
                            style={{ width: `${(rating / 10) * 100}%` }}
                          />
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Correlations */}
              {stats?.correlations?.overall_rating && (
                <div>
                  <h3 className="font-semibold text-gray-900 mb-4">Rating Correlations with Overall Score</h3>
                  <div className="bg-gray-50 rounded-xl p-4 border border-gray-200">
                    <div className="space-y-3">
                      {Object.entries(stats.correlations.overall_rating)
                        .filter(([key]) => key !== 'overall_rating' && key !== 'wifi_connectivity')
                        .slice(0, 6)
                        .map(([key, value]) => (
                          <div key={key}>
                            <div className="flex justify-between text-sm mb-1">
                              <span className="text-gray-700 capitalize">{key.replace(/_/g, ' ')}</span>
                              <span className={`font-semibold ${value > 0.7 ? 'text-green-600' : value > 0.4 ? 'text-yellow-600' : 'text-gray-600'}`}>
                                {(value * 100).toFixed(0)}%
                              </span>
                            </div>
                            <div className="w-full bg-gray-200 rounded-full h-1.5">
                              <div
                                className={`h-1.5 rounded-full transition-all duration-500 ${
                                  value > 0.7 ? 'bg-gradient-to-r from-green-400 to-emerald-500' :
                                  value > 0.4 ? 'bg-gradient-to-r from-yellow-400 to-orange-500' :
                                  'bg-gradient-to-r from-gray-400 to-gray-500'
                                }`}
                                style={{ width: `${Math.abs(value) * 100}%` }}
                              />
                            </div>
                          </div>
                        ))}
                    </div>
                  </div>
                </div>
              )}

              {/* Statistical Details */}
              <div>
                <h3 className="font-semibold text-gray-900 mb-4">Statistical Distribution</h3>
                <div className="grid md:grid-cols-2 gap-4">
                  {stats?.numeric && Object.entries(stats.numeric)
                    .filter(([key]) => key === 'overall_rating' || key === 'seat_comfort')
                    .map(([key, values]) => (
                      <div key={key} className="bg-gray-50 rounded-xl p-4 border border-gray-200">
                        <h4 className="font-medium text-gray-900 mb-3 capitalize">{key.replace(/_/g, ' ')}</h4>
                        <div className="grid grid-cols-3 gap-3">
                          <div>
                            <p className="text-xs text-gray-500">Mean</p>
                            <p className="text-lg font-bold text-gray-900">{values.mean?.toFixed(2)}</p>
                          </div>
                          <div>
                            <p className="text-xs text-gray-500">Median</p>
                            <p className="text-lg font-bold text-gray-900">{values.median}</p>
                          </div>
                          <div>
                            <p className="text-xs text-gray-500">Std Dev</p>
                            <p className="text-lg font-bold text-gray-900">{values.std?.toFixed(2)}</p>
                          </div>
                        </div>
                      </div>
                    ))}
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Reviews List */}
        <div>
          <div className="flex items-center justify-between mb-6">
            <h2 className="text-2xl font-bold text-gray-900 flex items-center">
              <Users className="h-6 w-6 mr-2 text-green-600" />
              Customer Reviews
            </h2>
            <div className="flex items-center space-x-2 bg-gray-100 px-4 py-2 rounded-full">
              <Activity className="h-4 w-4 text-gray-600" />
              <span className="text-sm font-medium text-gray-700">
                {display_rows?.length || 0} Total
              </span>
            </div>
          </div>
          
          <div className="space-y-4">
            {currentReviews.map((review, idx) => (
              <div
                key={review.id}
                data-review-index={idx}
                className={`group bg-white border border-gray-200 rounded-xl p-6 hover:shadow-lg hover:border-gray-300 transition-all duration-300 ${
                  animateStats ? 'opacity-100 translate-x-0' : 'opacity-0 translate-x-4'
                }`}
                style={{ transitionDelay: `${idx * 50}ms` }}
              >
                <div className="flex items-start gap-4">
                  {/* Rating Badge */}
                  <div className="flex-shrink-0">
                    <div className="relative">
                      <div className="bg-gradient-to-br from-blue-500 to-indigo-600 text-white font-bold text-xl w-16 h-16 rounded-2xl flex flex-col items-center justify-center shadow-md group-hover:shadow-lg transition-shadow">
                        <span className="text-2xl">{review.overall_rating || 'N/A'}</span>
                        <span className="text-[10px] opacity-90">/ 10</span>
                      </div>
                      {review.verified && (
                        <div className="absolute -top-1 -right-1 bg-green-500 rounded-full p-1">
                          <CheckCircle className="h-3 w-3 text-white" />
                        </div>
                      )}
                    </div>
                  </div>
                  
                  {/* Review Content */}
                  <div className="flex-1 min-w-0">
                    {/* Header */}
                    <div className="flex items-start justify-between gap-4 mb-3">
                      <div>
                        <h3 className="font-bold text-gray-900 text-lg mb-2">
                          {review.airline_name}
                        </h3>
                        
                        <div className="flex flex-wrap items-center gap-2">
                          <span className="inline-flex items-center bg-indigo-50 text-indigo-700 text-xs font-medium px-2.5 py-1 rounded-md">
                            {review.type_of_traveller}
                          </span>
                          <span className="inline-flex items-center bg-purple-50 text-purple-700 text-xs font-medium px-2.5 py-1 rounded-md">
                            {review.seat_type}
                          </span>
                          {review.recommended === 'yes' ? (
                            <span className="inline-flex items-center gap-1 bg-green-50 text-green-700 text-xs font-medium px-2.5 py-1 rounded-md">
                              <ThumbsUp className="h-3 w-3" />
                              Recommended
                            </span>
                          ) : (
                            <span className="inline-flex items-center gap-1 bg-red-50 text-red-700 text-xs font-medium px-2.5 py-1 rounded-md">
                              <XCircle className="h-3 w-3" />
                              Not Recommended
                            </span>
                          )}
                        </div>
                      </div>
                    </div>

                    {/* Review Text */}
                    <p className="text-gray-700 leading-relaxed mb-4 text-sm">
                      {review.review}
                    </p>

                    {/* Rating Breakdown */}
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-3 pt-3 border-t border-gray-100">
                      {[
                        { label: 'Seat Comfort', value: review.seat_comfort, icon: '💺' },
                        { label: 'Staff Service', value: review.cabin_staff_service, icon: '👥' },
                        { label: 'Food & Drinks', value: review.food_beverages, icon: '🍽️' },
                        { label: 'Value', value: review.value_for_money, icon: '💰' }
                      ].map((item, i) => (
                        <div key={i} className="flex items-center gap-2">
                          <span className="text-lg">{item.icon}</span>
                          <div className="flex-1 min-w-0">
                            <p className="text-xs text-gray-500 truncate">{item.label}</p>
                            <p className="text-sm font-bold text-gray-900">
                              {item.value || '-'}<span className="text-xs text-gray-400 font-normal">/5</span>
                            </p>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>

          {/* Load More Section */}
          {hasMore && (
            <div className="mt-8 relative">
              <div className="absolute inset-x-0 top-0 h-24 bg-gradient-to-b from-transparent via-slate-50/50 to-slate-50 pointer-events-none" />
              <div className="relative flex flex-col items-center pt-12">
                <button
                  onClick={loadMore}
                  className="group relative inline-flex items-center gap-3 px-8 py-4 bg-white border-2 border-gray-200 rounded-2xl hover:border-indigo-500 hover:shadow-lg transition-all duration-300"
                >
                  <div className="flex items-center gap-3">
                    <div className="bg-gradient-to-br from-indigo-500 to-blue-600 text-white rounded-xl p-2">
                      <ChevronDown className="h-5 w-5 group-hover:translate-y-1 transition-transform" />
                    </div>
                    <div className="text-left">
                      <div className="font-semibold text-gray-900 group-hover:text-indigo-600 transition-colors">
                        Load More Reviews
                      </div>
                      <div className="text-xs text-gray-500">
                        {display_rows.length - visibleCount} more available
                      </div>
                    </div>
                  </div>
                  <div className="ml-2 bg-indigo-50 text-indigo-700 px-3 py-1.5 rounded-lg text-sm font-semibold">
                    +{Math.min(reviewsPerPage, display_rows.length - visibleCount)}
                  </div>
                </button>
                
                <div className="mt-4 flex items-center gap-2">
                  <div className="h-1.5 w-24 bg-gray-200 rounded-full overflow-hidden">
                    <div 
                      className="h-full bg-gradient-to-r from-indigo-500 to-blue-600 rounded-full transition-all duration-500"
                      style={{ width: `${(visibleCount / display_rows.length) * 100}%` }}
                    />
                  </div>
                  <span className="text-xs text-gray-500 font-medium">
                    {visibleCount} / {display_rows.length}
                  </span>
                </div>
              </div>
            </div>
          )}
          
          {!hasMore && display_rows.length > 5 && (
            <div className="mt-8 text-center">
              <div className="inline-flex items-center gap-2 bg-gradient-to-r from-green-50 to-emerald-50 border border-green-200 px-6 py-3 rounded-xl">
                <CheckCircle className="h-5 w-5 text-green-600" />
                <p className="text-green-700 font-medium">
                  All {display_rows.length} reviews displayed
                </p>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default BaseCase;