"use client"

import { motion } from "framer-motion"
import { Quote, Star } from "lucide-react"
import Image from "next/image"

interface TestimonialProps {
  quote: string
  author: string
  role: string
  image: string
  rating: number
  delay?: number
}

export default function Testimonial({ quote, author, role, image, rating, delay = 0 }: TestimonialProps) {
  return (
    <motion.div
      className="flex flex-col p-8 bg-dark-100/80 backdrop-blur-md rounded-xl border border-white/10 relative overflow-hidden"
      initial={{ opacity: 0, y: 30 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true }}
      transition={{ duration: 0.5, delay }}
      whileHover={{
        y: -10,
        boxShadow: "0 10px 30px -10px rgba(157, 78, 221, 0.3)",
        transition: { duration: 0.3 },
      }}
    >
      <div className="absolute -top-20 -right-20 w-40 h-40 rounded-full bg-gradient-radial from-neon-purple/10 to-transparent blur-xl"></div>

      <Quote className="h-8 w-8 text-neon-purple mb-4" />

      <p className="mb-6 flex-1 text-muted-foreground">{quote}</p>

      <div className="flex items-center gap-4">
        <div className="h-12 w-12 rounded-full overflow-hidden border border-white/20">
          <Image src={image || "/placeholder.svg"} alt={author} width={48} height={48} className="object-cover" />
        </div>
        <div>
          <div className="font-display font-semibold neon-gradient-text-static">{author}</div>
          <div className="text-sm text-muted-foreground">{role}</div>
        </div>
      </div>

      <div className="absolute top-8 right-8 flex">
        {Array.from({ length: rating }).map((_, i) => (
          <Star key={i} className="h-4 w-4 fill-neon-yellow text-neon-yellow" />
        ))}
      </div>

      <motion.div
        className="absolute bottom-0 left-0 right-0 h-1 neon-gradient-bg opacity-0"
        whileHover={{ opacity: 1 }}
      />
    </motion.div>
  )
}

