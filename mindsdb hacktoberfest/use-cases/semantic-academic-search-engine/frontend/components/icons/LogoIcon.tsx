import React from 'react';

export const LogoIcon: React.FC<React.SVGProps<SVGSVGElement>> = (props) => (
  <svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" {...props}>
    <path d="M12 2L3.5 21h17L12 2z" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
    <path d="M8.5 15h7" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
    <path d="M12 7.5l.55 1.1.1.22h.23l1.22.18-.88 1.07-.2.24.05.27.21 1.21-1.1-.57-.23-.12-.23.12-1.1.57.2-1.2.05-.27-.2-.24-.9-1.07 1.22-.18h.23l.1-.22.55-1.1z" fill="currentColor" />
  </svg>
);