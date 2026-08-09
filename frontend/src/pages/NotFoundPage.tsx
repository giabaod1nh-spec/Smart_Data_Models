// NotFoundPage.tsx

import { useNavigate } from 'react-router-dom'
import { TriangleAlert } from 'lucide-react'

export function NotFoundPage() {
  const navigate = useNavigate()
  return (
    <div className="flex h-screen items-center justify-center" style={{ background: 'var(--bg-base)' }}>
      <div className="text-center">
        <TriangleAlert size={48} className="mx-auto mb-4" style={{ color: 'var(--text-muted)' }} />
        <div className="text-5xl font-bold mb-2" style={{ color: 'var(--blue)' }}>404</div>
        <div className="text-lg font-semibold mb-1" style={{ color: 'var(--text-primary)' }}>Page Not Found</div>
        <div className="text-sm mb-6" style={{ color: 'var(--text-muted)' }}>
          The page you are looking for does not exist.
        </div>
        <button
          onClick={() => navigate('/analytics')}
          className="px-6 py-2.5 rounded-lg text-sm font-semibold transition-colors"
          style={{ background: 'var(--blue)', color: 'white', border: 'none', cursor: 'pointer' }}
        >
          Back to Dashboard
        </button>
      </div>
    </div>
  )
}
