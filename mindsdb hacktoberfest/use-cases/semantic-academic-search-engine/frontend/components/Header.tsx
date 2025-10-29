import React from 'react';
import { GithubIcon } from './icons/GithubIcon';
import logo from './icons/logo.png';

interface HeaderProps {
  showTitle?: boolean;
  hideBorder?: boolean;
}

const Header: React.FC<HeaderProps> = ({ showTitle = false, hideBorder = false }) => {
  const headerClasses = `bg-white/80 backdrop-blur-sm sticky top-0 z-10 ${!hideBorder ? 'border-b border-gray-200' : ''}`;

  return (
    <header className={headerClasses}>
      <div className="container mx-auto px-4 py-3">
        <div className="flex items-center">
          <div className="flex-1">
            <img 
              src={logo} 
              alt="Semantica Logo" 
              className="h-8 w-8" 
            />
          </div>
          <div className="flex-1 text-center">
            {showTitle && (
              <h1 className="text-lg font-semibold text-gray-800">Semantica</h1>
            )}
          </div>
          <div className="flex-1 flex justify-end">
            <nav className="flex items-center space-x-4 text-sm text-gray-600">
              <a 
                href="https://github.com/Better-Boy/Semantica" 
                target="_blank" 
                rel="noopener noreferrer" 
                className="text-gray-500 hover:text-gray-900 transition-colors"
                aria-label="GitHub Repository"
              >
                <GithubIcon className="h-6 w-6" />
              </a>
            </nav>
          </div>
        </div>
      </div>
    </header>
  );
};

export default Header;