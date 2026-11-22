// router.tsx — Application routes

import { createBrowserRouter } from 'react-router-dom'
import { ProtectedRoute } from '@/features/auth/ProtectedRoute'
import { LoginPage } from '@/pages/LoginPage'
import { AnalyticsOverviewPage } from '@/pages/AnalyticsOverviewPage'
import { IntersectionDetailPage } from '@/pages/IntersectionDetailPage'
import { NotFoundPage } from '@/pages/NotFoundPage'
import { AppShell } from '@/components/layout/AppShell'
import { OperationsPage } from '@/pages/OperationsPage'
import { IntersectionsPage } from '@/pages/IntersectionsPage'
import { AdaptiveControlPage } from '@/pages/AdaptiveControlPage'
import { ExperimentsPage } from '@/pages/ExperimentsPage'
import { SystemMonitoringPage } from '@/pages/SystemMonitoringPage'

export const router = createBrowserRouter([
  {
    path: '/login',
    element: <LoginPage />,
  },
  {
    // Protected shell — all non-login routes live inside AppShell
    element: (
      <ProtectedRoute>
        <AppShell />
      </ProtectedRoute>
    ),
    children: [
      {
        path: '/operations',
        element: <OperationsPage />,
      },
      {
        path: '/intersections',
        element: <IntersectionsPage />,
      },
      {
        path: '/analytics',
        element: <AnalyticsOverviewPage />,
      },
      {
        path: '/intersections/:intersectionId',
        element: <IntersectionDetailPage />,
      },
      {
        path: '/adaptive-control',
        element: <AdaptiveControlPage />,
      },
      {
        path: '/experiments',
        element: <ExperimentsPage />,
      },
      {
        path: '/system',
        element: <SystemMonitoringPage />,
      },
      {
        path: '/',
        element: <OperationsPage />,
      },
    ],
  },
  {
    path: '*',
    element: <NotFoundPage />,
  },
])
