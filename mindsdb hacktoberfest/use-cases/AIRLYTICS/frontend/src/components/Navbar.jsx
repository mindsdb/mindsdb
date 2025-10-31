import { useState } from 'react';
import { Menu, X, BarChart3, Github, Linkedin, Sparkles } from 'lucide-react';
import { Link, useLocation } from 'react-router-dom';

const Navbar = () => {
  const [isOpen, setIsOpen] = useState(false);
  const location = useLocation();

  const navLinks = [
    { name: 'Landing', path: '/' },
    { name: 'Home', path: '/home' },
    { name: 'About', path: '/about' },
  ];

  const isActive = (path) => location.pathname === path;

  return (
    <nav className="fixed w-full bg-gradient-to-r from-slate-900 via-blue-950 to-slate-900 backdrop-blur-xl z-50 border-b border-blue-900/50 shadow-lg">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center h-20">
          {/* Logo - Left */}
          <Link to="/" className="flex items-center space-x-3 group">
            <div className="relative">
              <div className="absolute inset-0 bg-gradient-to-r from-violet-600 to-purple-600 rounded-xl blur opacity-75 group-hover:opacity-100 transition-opacity"></div>
              <div className="relative bg-gradient-to-r from-violet-600 to-purple-600 p-2.5 rounded-xl">
                <BarChart3 className="h-6 w-6 text-white" />
              </div>
            </div>
            <div className="flex flex-col">
              <span className="text-2xl font-bold text-white">
                AirLytics
              </span>
              <span className="text-xs text-gray-400 -mt-1">AI Analytics Platform</span>
            </div>
          </Link>

          {/* Center Navigation - Desktop */}
          <div className="hidden md:flex items-center space-x-1 bg-blue-950/60 rounded-full px-2 py-2 border border-blue-900/50">
            {navLinks.map((link) => (
              <Link
                key={link.path}
                to={link.path}
                className={`relative px-6 py-2.5 text-sm font-semibold rounded-full transition-all duration-300 ${
                  isActive(link.path)
                    ? 'bg-white text-violet-600 shadow-md'
                    : 'text-blue-200 hover:text-white hover:bg-blue-900/50'
                }`}
              >
                {link.name}
              </Link>
            ))}
          </div>

          {/* Right Side - Social + CTA */}
          <div className="hidden md:flex items-center space-x-4">
            <div className="flex items-center space-x-2">
              <a
                href="https://github.com/rajesh-adk-137"
                target="_blank"
                rel="noopener noreferrer"
                className="p-2.5 text-gray-500 hover:text-gray-900 hover:bg-gray-100 rounded-lg transition-all duration-300"
                aria-label="GitHub"
              >
                <Github className="h-5 w-5" />
              </a>
              <a
                href="https://www.linkedin.com/in/rajesh-adhikari-93425a310/"
                target="_blank"
                rel="noopener noreferrer"
                className="p-2.5 text-gray-500 hover:text-violet-600 hover:bg-violet-50 rounded-lg transition-all duration-300"
                aria-label="LinkedIn"
              >
                <Linkedin className="h-5 w-5" />
              </a>
            </div>
            <Link
              to="/home"
              className="inline-flex items-center space-x-2 bg-gradient-to-r from-violet-600 to-purple-600 text-white px-5 py-2.5 rounded-full font-semibold text-sm shadow-md hover:shadow-lg transform hover:scale-105 transition-all duration-300"
            >
              <Sparkles className="h-4 w-4" />
              <span>Try Now</span>
            </Link>
          </div>

          {/* Mobile Menu Button */}
          <button
            onClick={() => setIsOpen(!isOpen)}
            className="md:hidden p-2 rounded-lg text-gray-600 hover:bg-gray-100 transition-colors"
            aria-label="Toggle menu"
          >
            {isOpen ? <X className="h-6 w-6" /> : <Menu className="h-6 w-6" />}
          </button>
        </div>
      </div>

      {/* Mobile Menu */}
      {isOpen && (
        <div className="md:hidden bg-white/95 backdrop-blur-xl border-t border-gray-200">
          <div className="px-4 pt-4 pb-6 space-y-3">
            {navLinks.map((link) => (
              <Link
                key={link.path}
                to={link.path}
                onClick={() => setIsOpen(false)}
                className={`block px-4 py-3 rounded-xl text-sm font-semibold transition-all duration-300 ${
                  isActive(link.path)
                    ? 'bg-gradient-to-r from-violet-600 to-purple-600 text-white shadow-md'
                    : 'text-gray-700 hover:bg-gray-100'
                }`}
              >
                {link.name}
              </Link>
            ))}
            
            <div className="pt-4 border-t border-gray-200">
              <Link
                to="/home"
                className="flex items-center justify-center space-x-2 bg-gradient-to-r from-violet-600 to-purple-600 text-white px-5 py-3 rounded-xl font-semibold text-sm shadow-md w-full"
              >
                <Sparkles className="h-4 w-4" />
                <span>Try Now</span>
              </Link>
            </div>

            {/* Mobile Social Links */}
            <div className="flex items-center justify-center space-x-3 pt-3">
              <a
                href="https://github.com"
                target="_blank"
                rel="noopener noreferrer"
                className="p-2.5 text-gray-500 hover:text-gray-900 hover:bg-gray-100 rounded-lg transition-all"
                aria-label="GitHub"
              >
                <Github className="h-5 w-5" />
              </a>
              <a
                href="https://linkedin.com"
                target="_blank"
                rel="noopener noreferrer"
                className="p-2.5 text-gray-500 hover:text-violet-600 hover:bg-violet-50 rounded-lg transition-all"
                aria-label="LinkedIn"
              >
                <Linkedin className="h-5 w-5" />
              </a>
            </div>
          </div>
        </div>
      )}
    </nav>
  );
};

export default Navbar;