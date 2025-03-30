"use client"

import { motion } from "framer-motion"
import { Check } from "lucide-react"
import { Button } from "@/components/ui/button"

interface PricingCardProps {
  title: string
  price: string
  description: string
  features: string[]
  buttonText: string
  popular?: boolean
  delay?: number
}

export default function PricingCard({
  title,
  price,
  description,
  features,
  buttonText,
  popular = false,
  delay = 0,
}: PricingCardProps) {
  return (
    <motion.div
      className={`flex flex-col p-8 ${
        popular
          ? "bg-dark-100/90 backdrop-blur-xl border-neon-purple"
          : "bg-dark-100/80 backdrop-blur-md border-white/10"
      } rounded-xl border relative overflow-hidden`}
      initial={{ opacity: 0, y: 30 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true }}
      transition={{ duration: 0.5, delay }}
      whileHover={{
        y: -10,
        boxShadow: popular ? "0 10px 30px -10px rgba(157, 78, 221, 0.4)" : "0 10px 30px -10px rgba(255, 255, 255, 0.1)",
        transition: { duration: 0.3 },
      }}
    >
      {popular && (
        <div className="absolute -top-5 left-1/2 transform -translate-x-1/2 bg-neon-gradient-bg text-white text-sm font-medium py-1 px-4 rounded-full">
          Most Popular
        </div>
      )}

      <div className="absolute -top-20 -right-20 w-40 h-40 rounded-full bg-gradient-radial from-neon-purple/10 to-transparent blur-xl"></div>
      <div className="absolute -bottom-20 -left-20 w-40 h-40 rounded-full bg-gradient-radial from-neon-blue/10 to-transparent blur-xl"></div>

      <h3 className={`text-xl font-display font-bold mb-2 ${popular ? "neon-gradient-text" : ""}`}>{title}</h3>

      <div className="mb-4 flex items-baseline">
        <span className="text-3xl font-display font-bold">{price}</span>
        <span className="text-muted-foreground ml-1"> / month</span>
      </div>

      <p className="text-muted-foreground mb-6">{description}</p>

      <ul className="space-y-3 mb-8 flex-1">
        {features.map((feature, index) => (
          <motion.li
            key={index}
            className="flex items-start gap-2"
            initial={{ opacity: 0, x: -10 }}
            whileInView={{ opacity: 1, x: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.3, delay: delay + 0.1 + index * 0.05 }}
          >
            <Check className={`h-5 w-5 mt-0.5 ${popular ? "text-neon-teal" : "text-neon-purple"}`} />
            <span>{feature}</span>
          </motion.li>
        ))}
      </ul>

      <Button className={popular ? "neon-gradient-bg" : "neon-border"} variant={popular ? "default" : "outline"}>
        {buttonText}
      </Button>

      {popular && (
        <motion.div
          className="absolute inset-0 border-2 border-neon-purple rounded-xl pointer-events-none"
          animate={{
            boxShadow: [
              "0 0 0px rgba(157, 78, 221, 0.3)",
              "0 0 15px rgba(157, 78, 221, 0.5)",
              "0 0 0px rgba(157, 78, 221, 0.3)",
            ],
          }}
          transition={{ duration: 2, repeat: Number.POSITIVE_INFINITY }}
        />
      )}
    </motion.div>
  )
}

