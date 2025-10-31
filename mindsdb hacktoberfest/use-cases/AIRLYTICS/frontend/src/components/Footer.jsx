import { BarChart3, Heart, Github, Award } from 'lucide-react';

const Footer = () => {
  const currentYear = new Date().getFullYear();

  return (
    <footer className="bg-gradient-to-br from-gray-900 via-gray-800 to-gray-900 text-white">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-10">
        <div className="flex flex-col md:flex-row justify-between items-center space-y-6 md:space-y-0">
          <div className="flex items-center space-x-2">
            <div className="bg-gradient-to-br from-blue-500 to-indigo-500 p-2 rounded-lg">
              <BarChart3 className="h-5 w-5 text-white" />
            </div>
            <div>
              <span className="text-xl font-bold">AirLytics</span>
              <p className="text-xs text-gray-400">AI-Powered Airline Intelligence</p>
            </div>
          </div>

          <div className="flex items-center space-x-6">
            <div className="flex items-center space-x-2 text-gray-400 text-sm">
              <Award className="h-4 w-4 text-orange-400" />
              <span>MindsDB Hacktoberfest 2025</span>
            </div>
            <div className="flex items-center space-x-2 text-gray-400 text-sm">
              <Github className="h-4 w-4" />
              <a href="https://github.com" className="hover:text-white transition-colors">
                Open Source
              </a>
            </div>
          </div>

          <div className="flex items-center space-x-2 text-gray-400 text-sm">
            <span>Built with</span>
            <Heart className="h-4 w-4 text-red-500 fill-red-500 animate-pulse" />
            <span>for data-driven decisions</span>
          </div>
        </div>

        <div className="border-t border-gray-700 mt-8 pt-6">
          <div className="flex flex-col md:flex-row justify-between items-center space-y-3 md:space-y-0">
            <p className="text-center text-gray-500 text-xs">
              © {currentYear} AirLytics. Powered by MindsDB Knowledge Bases, FastAPI & React
            </p>
            <p className="text-center text-gray-600 text-xs">
              Zero-ETL Analytics • Hybrid Semantic Search • Agent-Driven Intelligence
            </p>
          </div>
        </div>
      </div>
    </footer>
  );
};

export default Footer;