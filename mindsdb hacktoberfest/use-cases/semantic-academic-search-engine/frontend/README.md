# Semantica Frontend

React-based frontend for the Semantica academic search and chat platform.

## 🎨 Overview

The Semantica frontend is a modern, responsive web application built with React 19.2 and TypeScript. It provides an intuitive interface for searching academic papers and chatting with AI about selected documents.

## 🛠️ Tech Stack

- **React 19.2** - UI library
- **TypeScript 5.8** - Type safety
- **Vite 6.2** - Build tool and dev server
- **Axios 1.13** - HTTP client
- **React Markdown 9.0** - Markdown rendering for AI responses
- **Tailwind CSS** - Utility-first CSS (via classes)

## 📂 Project Structure

```
frontend/
├── components/              # Reusable UI components
│   ├── icons/              # SVG icon components
│   │   ├── ArrowLeftIcon.tsx
│   │   ├── BookmarkIcon.tsx
│   │   ├── ChatIcon.tsx
│   │   ├── ChevronDownIcon.tsx
│   │   ├── ChevronUpIcon.tsx
│   │   ├── ExternalLinkIcon.tsx
│   │   ├── GithubIcon.tsx
│   │   ├── LogoIcon.tsx
│   │   ├── SearchIcon.tsx
│   │   ├── SendIcon.tsx
│   │   ├── SettingsIcon.tsx
│   │   ├── SunIcon.tsx
│   │   └── XIcon.tsx
│   ├── ChatWindow.tsx      # Chat interface component
│   ├── Header.tsx          # Application header
│   ├── PaperCard.tsx       # Individual paper display card
│   ├── ResultsGrid.tsx     # Grid layout for search results
│   ├── SearchBar.tsx       # Search input with filters
│   └── SelectedPapersList.tsx  # Bottom panel for selected papers
├── pages/                  # Page-level components
│   ├── ChatPage.tsx        # Chat interface page
│   └── SearchPage.tsx      # Search interface page
├── App.tsx                 # Main application component
├── index.tsx               # Application entry point
├── types.ts                # TypeScript type definitions
├── constants.ts            # Application constants
├── index.html              # HTML template
├── package.json            # Dependencies and scripts
├── tsconfig.json           # TypeScript configuration
├── vite.config.ts          # Vite configuration
└── .env                    # Environment variables (not in git)
```

## 🚀 Getting Started

### Prerequisites

- Node.js 18+ and npm
- Backend API running (see `../backend/README.md`)

### Installation

```bash
# Install dependencies
npm install

# Create environment file
echo "VITE_API_BASE_URL=http://localhost:8000/api/v1" > .env

# Start development server
npm run dev
```

The application will be available at `http://localhost:5173`

## 🔧 Configuration

### Environment Variables

Create a `.env` file in the frontend directory:

```env
VITE_API_BASE_URL=http://localhost:8000/api/v1
```

**Important**: The `VITE_` prefix is required for Vite to expose the variable to the client.

### Vite Configuration

The `vite.config.ts` file configures the build process:

```typescript
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173
  }
})
```

## 📝 Available Scripts

```bash
# Start development server with hot reload
npm run dev

# Build for production
npm run build

# Preview production build locally
npm run preview

# Run linter (if configured)
npm run lint
```

## 🐛 Common Issues

### API Connection Failed
- Check that backend is running on correct port
- Verify `VITE_API_BASE_URL` in `.env`
- Check browser console for CORS errors

### Build Errors
- Clear node_modules: `rm -rf node_modules && npm install`
- Clear Vite cache: `rm -rf node_modules/.vite`
- Check Node.js version: `node --version` (should be 18+)

### TypeScript Errors
- Run type check: `npx tsc --noEmit`
- Check `tsconfig.json` configuration
- Ensure all dependencies have type definitions

## 📚 Additional Resources

- [React Documentation](https://react.dev/)
- [TypeScript Handbook](https://www.typescriptlang.org/docs/)
- [Vite Guide](https://vitejs.dev/guide/)
- [Tailwind CSS](https://tailwindcss.com/docs)
- [Axios Documentation](https://axios-http.com/docs/intro)

---

**Made with ❤️ for MindsDB Hacktoberfest**
