"use client"

import type { ReactNode } from "react"
import { motion } from "framer-motion"

interface FeatureCardProps {
  icon: ReactNode
  title: string
  description: string
  color?: "purple" | "blue" | "pink" | "teal" | "yellow"
  delay?: number
}

export default function FeatureCard({ icon, title, description, color = "purple", delay = 0 }: FeatureCardProps) {
  const colorClasses = {
    purple: "text-neon-purple",
    blue: "text-neon-blue",
    pink: "text-neon-pink",
    teal: "text-neon-teal",
    yellow: "text-neon-yellow",
  }

  const glowClasses = {
    purple: "neon-glow",
    blue: "neon-glow-blue",
    pink: "neon-glow-pink",
    teal: "neon-glow-teal",
    yellow: "neon-glow",
  }

  const bgClasses = {
    purple: "bg-neon-purple/10",
    blue: "bg-neon-blue/10",
    pink: "bg-neon-pink/10",
    teal: "bg-neon-teal/10",
    yellow: "bg-neon-yellow/10",
  }

  return (
    <motion.div
      className="flex flex-col items-center text-center p-8 bg-dark-100/80 backdrop-blur-md rounded-xl border border-white/10 relative overflow-hidden"
      initial={{ opacity: 0, y: 30 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true }}
      transition={{ duration: 0.5, delay }}
      whileHover={{
        y: -10,
        transition: { duration: 0.3 },
      }}
    >
      <div className="absolute -top-20 -right-20 w-40 h-40 rounded-full bg-gradient-radial from-neon-purple/10 to-transparent blur-xl"></div>
      <div className="absolute -bottom-20 -left-20 w-40 h-40 rounded-full bg-gradient-radial from-neon-blue/10 to-transparent blur-xl"></div>

      <motion.div
        className={`h-16 w-16 rounded-2xl ${bgClasses[color]} flex items-center justify-center mb-6 ${glowClasses[color]} relative z-10`}
        whileHover={{ rotate: [0, -10, 10, -10, 0] }}
        transition={{ duration: 0.5 }}
      >
        <div className={colorClasses[color]}>{icon}</div>
      </motion.div>

      <h3 className={`text-xl font-display font-bold mb-3 ${colorClasses[color]}`}>{title}</h3>

      <p className="text-muted-foreground">{description}</p>

      <motion.div
        className="absolute bottom-0 left-0 right-0 h-1 neon-gradient-bg opacity-0"
        whileHover={{ opacity: 1 }}
      />
    </motion.div>
  )
}

