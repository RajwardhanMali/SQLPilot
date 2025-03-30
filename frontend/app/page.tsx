"use client";

import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { motion, useScroll, useTransform, useInView } from "framer-motion";
import {
  ArrowRight,
  Database,
  Code,
  Sparkles,
  Zap,
  BarChart3,
  MessageSquare,
  ChevronDown,
  Star,
  Check,
  ArrowUpRight,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import HeroAnimation from "@/components/landing/hero-animation";
import FeatureCard from "@/components/landing/feature-card";
import Testimonial from "@/components/landing/testimonial";
import PricingCard from "@/components/landing/pricing-card";
import { cn } from "@/lib/utils";
import { signOut, useSession } from "next-auth/react";

export default function LandingPage() {
  const [activeSection, setActiveSection] = useState("hero");
  const heroRef = useRef<HTMLDivElement>(null);
  const featuresRef = useRef<HTMLDivElement>(null);
  const howItWorksRef = useRef<HTMLDivElement>(null);
  const testimonialsRef = useRef<HTMLDivElement>(null);
  const pricingRef = useRef<HTMLDivElement>(null);
  const ctaRef = useRef<HTMLDivElement>(null);

  const heroInView = useInView(heroRef, { amount: 0.5 });
  const featuresInView = useInView(featuresRef, { amount: 0.5 });
  const howItWorksInView = useInView(howItWorksRef, { amount: 0.5 });
  const testimonialsInView = useInView(testimonialsRef, { amount: 0.5 });
  const pricingInView = useInView(pricingRef, { amount: 0.5 });
  const ctaInView = useInView(ctaRef, { amount: 0.5 });

  const { data: session } = useSession();

  useEffect(() => {
    if (heroInView) setActiveSection("hero");
    else if (featuresInView) setActiveSection("features");
    else if (howItWorksInView) setActiveSection("how-it-works");
    else if (testimonialsInView) setActiveSection("testimonials");
    else if (pricingInView) setActiveSection("pricing");
    else if (ctaInView) setActiveSection("cta");
  }, [
    heroInView,
    featuresInView,
    howItWorksInView,
    testimonialsInView,
    pricingInView,
    ctaInView,
  ]);

  const { scrollYProgress } = useScroll();
  const headerOpacity = useTransform(scrollYProgress, [0, 0.05], [0, 1]);

  const scrollToSection = (sectionId: string) => {
    const section = document.getElementById(sectionId);
    if (section) {
      section.scrollIntoView({ behavior: "smooth" });
    }
  };

  const navItems = [
    { id: "features", label: "Features" },
    { id: "how-it-works", label: "How It Works" },
    { id: "testimonials", label: "Testimonials" },
    { id: "pricing", label: "Pricing" },
  ];

  return (
    <div className="flex min-h-screen flex-col dark">
      <motion.header
        className="fixed top-0 z-50 w-full border-b border-white/10 bg-dark-200/80 backdrop-blur-md"
        style={{ opacity: headerOpacity }}
        initial={{ opacity: 0 }}
      >
        <div className="container flex h-16 items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="h-8 w-8 rounded-lg neon-gradient-bg flex items-center justify-center text-white">
              <Database className="h-5 w-5" />
            </div>
            <span className="text-xl font-display font-bold neon-gradient-text">
              SQLPilot
            </span>
          </div>
          <nav className="hidden md:flex items-center gap-6">
            {navItems.map((item) => (
              <button
                key={item.id}
                onClick={() => scrollToSection(item.id)}
                className={cn(
                  "text-sm font-medium transition-all duration-300 hover:text-primary relative",
                  activeSection === item.id
                    ? "text-primary"
                    : "text-muted-foreground"
                )}
              >
                {item.label}
                {activeSection === item.id && (
                  <motion.div
                    layoutId="activeSection"
                    className="absolute -bottom-1 left-0 right-0 h-0.5 bg-primary"
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    transition={{ duration: 0.3 }}
                  />
                )}
              </button>
            ))}
          </nav>
          <div className="flex items-center gap-4">
            {session?.user ? (
              <>
                <Button onClick={() => signOut()} variant="ghost" size="sm">
                  Logout
                </Button>

                <Button asChild>
                  <Link href={"/dashboard"}>Dashboard</Link>
                </Button>
              </>
            ) : (
              <>
                <Link href="/login">
                  <Button
                    variant="ghost"
                    size="sm"
                    className="hover:text-primary"
                  >
                    Log in
                  </Button>
                </Link>
                <Link href="/register">
                  <Button
                    size="sm"
                    className="neon-gradient-bg hover:opacity-90 transition-opacity"
                  >
                    Get Started
                  </Button>
                </Link>
              </>
            )}
          </div>
        </div>
      </motion.header>

      <main className="flex-1 pt-16">
        <section
          id="hero"
          ref={heroRef}
          className="relative overflow-hidden py-20 md:py-32"
        >
          <div className="absolute inset-0 bg-dark-300 noise-bg"></div>
          <div className="absolute inset-0 bg-gradient-radial from-neon-purple/20 via-transparent to-transparent"></div>
          <div className="absolute top-1/4 right-1/4 w-[500px] h-[500px] rounded-full bg-neon-blue/5 blur-3xl"></div>
          <div className="absolute bottom-1/4 left-1/4 w-[300px] h-[300px] rounded-full bg-neon-pink/5 blur-3xl"></div>

          <div className="container relative z-10">
            <motion.div
              className="flex flex-col items-center text-center"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.5 }}
            >
              <motion.div
                className="inline-block mb-6 px-4 py-1.5 rounded-full border neon-border bg-dark-200/80"
                initial={{ opacity: 0, scale: 0.9 }}
                animate={{ opacity: 1, scale: 1 }}
                transition={{ delay: 0.2, duration: 0.5 }}
              >
                <span className="text-sm font-medium flex items-center gap-2">
                  <Sparkles className="h-4 w-4 text-neon-purple" />
                  AI-Powered SQL Assistant
                </span>
              </motion.div>

              <motion.h1
                className="text-4xl md:text-6xl lg:text-7xl font-display font-bold tracking-tight"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: 0.3, duration: 0.5 }}
              >
                Transform Natural Language into
                <span className="block neon-gradient-text mt-2">
                  Optimized SQL Queries
                </span>
              </motion.h1>

              <motion.p
                className="mt-6 max-w-3xl text-lg text-muted-foreground"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: 0.4, duration: 0.5 }}
              >
                Design schemas, generate DDL/DML statements, and execute queries
                in Trino or Spark SQL - all with the power of AI and a
                beautiful, intuitive interface.
              </motion.p>

              <motion.div
                className="mt-8 flex flex-wrap justify-center gap-4"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: 0.5, duration: 0.5 }}
              >
                <Link href="/register">
                  <Button
                    size="lg"
                    className="gap-2 neon-gradient-bg hover:opacity-90 transition-opacity shadow-lg"
                  >
                    Get Started <ArrowRight className="h-4 w-4" />
                  </Button>
                </Link>
                <Button
                  size="lg"
                  variant="outline"
                  className="neon-border"
                  onClick={() => scrollToSection("features")}
                >
                  Learn More
                </Button>
              </motion.div>

              <motion.div
                className="mt-8"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: 0.6, duration: 0.5 }}
              >
                <motion.div
                  className="flex items-center justify-center gap-2 text-muted-foreground cursor-pointer"
                  onClick={() => scrollToSection("features")}
                  whileHover={{ y: 5 }}
                  animate={{ y: [0, 5, 0] }}
                  transition={{ repeat: Number.POSITIVE_INFINITY, duration: 2 }}
                >
                  <span>Scroll to explore</span>
                  <ChevronDown className="h-4 w-4" />
                </motion.div>
              </motion.div>
            </motion.div>

            <motion.div
              className="mt-16 md:mt-24 relative z-10"
              initial={{ opacity: 0, y: 40 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.7, duration: 0.7 }}
            >
              <HeroAnimation />
            </motion.div>
          </div>
        </section>

        <section
          id="features"
          ref={featuresRef}
          className="py-24 sm:py-32 relative overflow-hidden"
        >
          <div className="absolute inset-0 bg-dark-200 noise-bg"></div>
          <div className="absolute inset-0 grid-pattern"></div>
          <div className="absolute top-1/3 left-1/4 w-[400px] h-[400px] rounded-full bg-neon-pink/5 blur-3xl"></div>

          <div className="container relative z-10">
            <div className="flex flex-col items-center text-center mb-16">
              <motion.div
                className="inline-block mb-6 px-4 py-1.5 rounded-full border neon-border bg-dark-200/80"
                initial={{ opacity: 0, scale: 0.9 }}
                whileInView={{ opacity: 1, scale: 1 }}
                viewport={{ once: true }}
                transition={{ duration: 0.5 }}
              >
                <span className="text-sm font-medium">Powerful Features</span>
              </motion.div>

              <motion.h2
                className="text-3xl md:text-5xl font-display font-bold neon-gradient-text"
                initial={{ opacity: 0, y: 20 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true }}
                transition={{ duration: 0.5, delay: 0.1 }}
              >
                Everything You Need
              </motion.h2>

              <motion.p
                className="mt-4 max-w-[85%] text-lg text-muted-foreground"
                initial={{ opacity: 0, y: 20 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true }}
                transition={{ duration: 0.5, delay: 0.2 }}
              >
                Master database design and SQL queries with our comprehensive
                toolset
              </motion.p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">
              <FeatureCard
                icon={<Database />}
                title="Schema Design"
                description="Generate optimal database schemas from natural language descriptions for OLAP use cases."
                color="purple"
                delay={0.1}
              />
              <FeatureCard
                icon={<Code />}
                title="SQL Generation"
                description="Automatically create DDL and DML statements based on your schema design."
                color="blue"
                delay={0.2}
              />
              <FeatureCard
                icon={<MessageSquare />}
                title="Natural Language to SQL"
                description="Translate English text into SQL queries in Trino or Spark SQL dialects."
                color="pink"
                delay={0.3}
              />
              <FeatureCard
                icon={<Sparkles />}
                title="SQL Completion"
                description="Get intelligent code completion as you write SQL queries."
                color="teal"
                delay={0.4}
              />
              <FeatureCard
                icon={<Zap />}
                title="Query Execution"
                description="Execute and validate your queries directly in the platform."
                color="yellow"
                delay={0.5}
              />
              <FeatureCard
                icon={<BarChart3 />}
                title="Performance Optimization"
                description="Get suggestions for storage-optimized and performance-efficient schemas."
                color="purple"
                delay={0.6}
              />
            </div>
          </div>
        </section>

        <section
          id="how-it-works"
          ref={howItWorksRef}
          className="py-24 sm:py-32 relative overflow-hidden"
        >
          <div className="absolute inset-0 bg-dark-300 noise-bg"></div>
          <div className="absolute inset-0 dot-pattern"></div>
          <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[600px] rounded-full bg-neon-blue/5 blur-3xl"></div>

          <div className="container relative z-10">
            <div className="flex flex-col items-center text-center mb-16">
              <motion.h2
                className="text-3xl md:text-5xl font-display font-bold neon-gradient-text"
                initial={{ opacity: 0, y: 20 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true }}
                transition={{ duration: 0.5 }}
              >
                How It Works
              </motion.h2>

              <motion.p
                className="mt-4 max-w-[85%] text-lg text-muted-foreground"
                initial={{ opacity: 0, y: 20 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true }}
                transition={{ duration: 0.5, delay: 0.1 }}
              >
                Our AI-powered platform simplifies database design and SQL query
                generation
              </motion.p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-8 relative">
              <div className="absolute top-1/2 left-0 right-0 h-0.5 bg-gradient-to-r from-neon-purple via-neon-pink to-neon-blue hidden md:block"></div>

              <motion.div
                className="flex flex-col items-center text-center p-8 bg-dark-100/80 backdrop-blur-md rounded-xl border border-white/10 relative"
                initial={{ opacity: 0, y: 30 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true }}
                transition={{ duration: 0.5, delay: 0.1 }}
                whileHover={{
                  y: -5,
                  boxShadow: "0 10px 30px -10px rgba(157, 78, 221, 0.3)",
                }}
              >
                <div className="absolute -top-5 left-1/2 transform -translate-x-1/2 w-10 h-10 rounded-full neon-gradient-bg flex items-center justify-center text-white font-bold z-10">
                  1
                </div>
                <div className="h-16 w-16 rounded-full bg-neon-purple/10 flex items-center justify-center mb-6 neon-glow">
                  <MessageSquare className="h-8 w-8 text-neon-purple" />
                </div>
                <h3 className="text-xl font-display font-bold mb-3 neon-text">
                  Describe Your Needs
                </h3>
                <p className="text-muted-foreground">
                  Tell us what you're trying to build in plain English. Our AI
                  understands your requirements and translates them into
                  technical specifications.
                </p>
              </motion.div>

              <motion.div
                className="flex flex-col items-center text-center p-8 bg-dark-100/80 backdrop-blur-md rounded-xl border border-white/10 relative"
                initial={{ opacity: 0, y: 30 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true }}
                transition={{ duration: 0.5, delay: 0.3 }}
                whileHover={{
                  y: -5,
                  boxShadow: "0 10px 30px -10px rgba(0, 180, 216, 0.3)",
                }}
              >
                <div className="absolute -top-5 left-1/2 transform -translate-x-1/2 w-10 h-10 rounded-full neon-gradient-bg flex items-center justify-center text-white font-bold z-10">
                  2
                </div>
                <div className="h-16 w-16 rounded-full bg-neon-blue/10 flex items-center justify-center mb-6 neon-glow-blue">
                  <Code className="h-8 w-8 text-neon-blue" />
                </div>
                <h3 className="text-xl font-display font-bold mb-3 neon-text-blue">
                  AI Generates SQL
                </h3>
                <p className="text-muted-foreground">
                  Our AI creates optimized SQL in your preferred dialect,
                  handling complex joins, aggregations, and performance
                  optimizations automatically.
                </p>
              </motion.div>

              <motion.div
                className="flex flex-col items-center text-center p-8 bg-dark-100/80 backdrop-blur-md rounded-xl border border-white/10 relative"
                initial={{ opacity: 0, y: 30 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true }}
                transition={{ duration: 0.5, delay: 0.5 }}
                whileHover={{
                  y: -5,
                  boxShadow: "0 10px 30px -10px rgba(255, 60, 162, 0.3)",
                }}
              >
                <div className="absolute -top-5 left-1/2 transform -translate-x-1/2 w-10 h-10 rounded-full neon-gradient-bg flex items-center justify-center text-white font-bold z-10">
                  3
                </div>
                <div className="h-16 w-16 rounded-full bg-neon-pink/10 flex items-center justify-center mb-6 neon-glow-pink">
                  <Zap className="h-8 w-8 text-neon-pink" />
                </div>
                <h3 className="text-xl font-display font-bold mb-3 neon-text-pink">
                  Execute & Refine
                </h3>
                <p className="text-muted-foreground">
                  Run your queries directly in the platform, visualize results,
                  and provide feedback to continuously improve the AI's
                  understanding of your needs.
                </p>
              </motion.div>
            </div>

            <motion.div
              className="mt-20 p-8 bg-dark-100/80 backdrop-blur-md rounded-xl border border-white/10 flex flex-col md:flex-row items-center gap-8"
              initial={{ opacity: 0, y: 30 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ duration: 0.5, delay: 0.7 }}
            >
              <div className="flex-1">
                <h3 className="text-2xl font-display font-bold mb-4 neon-gradient-text">
                  Continuous Learning
                </h3>
                <p className="text-muted-foreground mb-4">
                  SQLPilot gets smarter with every interaction. Our AI learns
                  from your feedback and adapts to your specific database needs
                  and query patterns.
                </p>
                <ul className="space-y-2">
                  {[
                    "Remembers your schema preferences",
                    "Adapts to your SQL style",
                    "Improves accuracy over time",
                    "Suggests optimizations based on usage patterns",
                  ].map((item, index) => (
                    <motion.li
                      key={index}
                      className="flex items-start gap-2"
                      initial={{ opacity: 0, x: -20 }}
                      whileInView={{ opacity: 1, x: 0 }}
                      viewport={{ once: true }}
                      transition={{ duration: 0.3, delay: 0.8 + index * 0.1 }}
                    >
                      <Check className="h-5 w-5 text-neon-teal mt-0.5" />
                      <span>{item}</span>
                    </motion.li>
                  ))}
                </ul>
              </div>
              <div className="flex-1 flex justify-center">
                <motion.div
                  className="relative w-64 h-64"
                  animate={{ rotate: 360 }}
                  transition={{
                    duration: 20,
                    repeat: Number.POSITIVE_INFINITY,
                    ease: "linear",
                  }}
                >
                  <div className="absolute inset-0 rounded-full bg-gradient-to-r from-neon-purple via-neon-pink to-neon-blue opacity-30 blur-md"></div>
                  <div className="absolute inset-4 rounded-full bg-dark-200 flex items-center justify-center">
                    <motion.div
                      className="text-6xl font-display font-bold neon-gradient-text"
                      animate={{ scale: [1, 1.05, 1] }}
                      transition={{
                        duration: 3,
                        repeat: Number.POSITIVE_INFINITY,
                      }}
                    >
                      AI
                    </motion.div>
                  </div>
                  {[0, 60, 120, 180, 240, 300].map((rotation, i) => (
                    <motion.div
                      key={i}
                      className="absolute top-0 left-1/2 w-2 h-2 rounded-full bg-neon-teal"
                      style={{
                        rotate: rotation,
                        transformOrigin: "0 32px",
                        x: 30,
                      }}
                      animate={{ scale: [1, 1.5, 1] }}
                      transition={{
                        duration: 2,
                        delay: i * 0.3,
                        repeat: Number.POSITIVE_INFINITY,
                      }}
                    />
                  ))}
                </motion.div>
              </div>
            </motion.div>
          </div>
        </section>

        <section
          id="testimonials"
          ref={testimonialsRef}
          className="py-24 sm:py-32 relative overflow-hidden"
        >
          <div className="absolute inset-0 bg-dark-200 noise-bg"></div>
          <div className="absolute inset-0 grid-pattern"></div>
          <div className="absolute top-1/2 right-1/4 w-[400px] h-[400px] rounded-full bg-neon-teal/5 blur-3xl"></div>

          <div className="container relative z-10">
            <div className="flex flex-col items-center text-center mb-16">
              <motion.div
                className="inline-block mb-6 px-4 py-1.5 rounded-full border neon-border bg-dark-200/80"
                initial={{ opacity: 0, scale: 0.9 }}
                whileInView={{ opacity: 1, scale: 1 }}
                viewport={{ once: true }}
                transition={{ duration: 0.5 }}
              >
                <span className="text-sm font-medium flex items-center gap-2">
                  <Star className="h-4 w-4 text-neon-yellow" />
                  Testimonials
                </span>
              </motion.div>

              <motion.h2
                className="text-3xl md:text-5xl font-display font-bold neon-gradient-text"
                initial={{ opacity: 0, y: 20 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true }}
                transition={{ duration: 0.5, delay: 0.1 }}
              >
                Loved by Data Teams
              </motion.h2>

              <motion.p
                className="mt-4 max-w-[85%] text-lg text-muted-foreground"
                initial={{ opacity: 0, y: 20 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true }}
                transition={{ duration: 0.5, delay: 0.2 }}
              >
                See what our users have to say about SQLPilot
              </motion.p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
              <Testimonial
                quote="SQLPilot has cut our schema design time in half. The AI suggestions are spot on and the visualizations help our team collaborate more effectively."
                author="Sarah Chen"
                role="Data Engineer at TechCorp"
                image="/placeholder.svg?height=100&width=100"
                rating={5}
                delay={0.1}
              />
              <Testimonial
                quote="The natural language to SQL feature is a game-changer for our analysts who aren't SQL experts. We've seen a 40% increase in self-service analytics adoption."
                author="Michael Rodriguez"
                role="Analytics Lead at DataDrive"
                image="/placeholder.svg?height=100&width=100"
                rating={5}
                delay={0.3}
              />
              <Testimonial
                quote="We've seen a 40% improvement in query performance thanks to the optimization suggestions. The platform learns from our patterns and keeps getting better."
                author="Priya Sharma"
                role="Database Architect at CloudScale"
                image="/placeholder.svg?height=100&width=100"
                rating={5}
                delay={0.5}
              />
            </div>

            <motion.div
              className="mt-16 p-8 bg-dark-100/80 backdrop-blur-md rounded-xl border border-white/10"
              initial={{ opacity: 0, y: 30 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ duration: 0.5, delay: 0.7 }}
            >
              <div className="flex flex-col md:flex-row items-center gap-8">
                <div className="flex-1">
                  <div className="flex items-center gap-2 mb-4">
                    {[1, 2, 3, 4, 5].map((star) => (
                      <Star
                        key={star}
                        className="h-6 w-6 fill-neon-yellow text-neon-yellow"
                      />
                    ))}
                    <span className="text-lg font-medium ml-2">4.9/5</span>
                  </div>
                  <h3 className="text-2xl font-display font-bold mb-2">
                    From 500+ verified customer reviews
                  </h3>
                  <p className="text-muted-foreground">
                    Join the community of data professionals who are
                    transforming their SQL workflows with SQLPilot.
                  </p>
                </div>
                <div className="flex-1 grid grid-cols-2 md:grid-cols-4 gap-4">
                  {["G2 Crowd", "Capterra", "Product Hunt", "TrustPilot"].map(
                    (platform, index) => (
                      <motion.div
                        key={platform}
                        className="flex flex-col items-center justify-center p-4 bg-dark-200/80 rounded-lg border border-white/10"
                        initial={{ opacity: 0, y: 20 }}
                        whileInView={{ opacity: 1, y: 0 }}
                        viewport={{ once: true }}
                        transition={{ duration: 0.3, delay: 0.8 + index * 0.1 }}
                        whileHover={{ y: -5 }}
                      >
                        <div className="text-xl font-bold neon-text">
                          {platform}
                        </div>
                        <div className="flex items-center mt-2">
                          <Star className="h-4 w-4 fill-neon-yellow text-neon-yellow" />
                          <span className="ml-1 text-sm">4.9</span>
                        </div>
                      </motion.div>
                    )
                  )}
                </div>
              </div>
            </motion.div>
          </div>
        </section>

        <section
          id="pricing"
          ref={pricingRef}
          className="py-24 sm:py-32 relative overflow-hidden"
        >
          <div className="absolute inset-0 bg-dark-300 noise-bg"></div>
          <div className="absolute inset-0 dot-pattern"></div>
          <div className="absolute top-1/3 left-1/3 w-[500px] h-[500px] rounded-full bg-neon-purple/5 blur-3xl"></div>

          <div className="container relative z-10">
            <div className="flex flex-col items-center text-center mb-16">
              <motion.h2
                className="text-3xl md:text-5xl font-display font-bold neon-gradient-text"
                initial={{ opacity: 0, y: 20 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true }}
                transition={{ duration: 0.5 }}
              >
                Simple Pricing
              </motion.h2>

              <motion.p
                className="mt-4 max-w-[85%] text-lg text-muted-foreground"
                initial={{ opacity: 0, y: 20 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true }}
                transition={{ duration: 0.5, delay: 0.1 }}
              >
                Choose the plan that's right for your team
              </motion.p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-8 max-w-5xl mx-auto">
              <PricingCard
                title="Starter"
                price="$0"
                description="Perfect for individuals and small projects"
                features={[
                  "Basic schema design",
                  "Simple SQL generation",
                  "Limited queries per day",
                  "Community support",
                ]}
                buttonText="Get Started"
                popular={false}
                delay={0.1}
              />

              <PricingCard
                title="Pro"
                price="$49"
                description="Ideal for teams and growing businesses"
                features={[
                  "Advanced schema design",
                  "Full SQL dialect support",
                  "Query execution",
                  "Unlimited queries",
                  "Priority support",
                  "Team collaboration",
                ]}
                buttonText="Get Started"
                popular={true}
                delay={0.3}
              />

              <PricingCard
                title="Enterprise"
                price="$199"
                description="For organizations with advanced needs"
                features={[
                  "Everything in Pro",
                  "Custom SQL dialect support",
                  "Advanced optimizations",
                  "Dedicated account manager",
                  "Custom integrations",
                  "SLA guarantees",
                ]}
                buttonText="Contact Sales"
                popular={false}
                delay={0.5}
              />
            </div>

            <motion.div
              className="mt-16 p-8 bg-dark-100/80 backdrop-blur-md rounded-xl border border-white/10 flex flex-col md:flex-row items-center gap-8"
              initial={{ opacity: 0, y: 30 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ duration: 0.5, delay: 0.7 }}
            >
              <div className="flex-1">
                <h3 className="text-2xl font-display font-bold mb-2">
                  Need a custom solution?
                </h3>
                <p className="text-muted-foreground">
                  We offer tailored enterprise plans for organizations with
                  specific requirements. Contact our sales team to discuss your
                  needs.
                </p>
              </div>
              <div>
                <Button size="lg" className="neon-gradient-bg">
                  <span className="flex items-center gap-2">
                    Contact Sales <ArrowUpRight className="h-4 w-4" />
                  </span>
                </Button>
              </div>
            </motion.div>
          </div>
        </section>

        <section
          id="cta"
          ref={ctaRef}
          className="py-24 sm:py-32 relative overflow-hidden"
        >
          <div className="absolute inset-0 bg-dark-200 noise-bg"></div>
          <div className="absolute inset-0 grid-pattern"></div>
          <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[600px] rounded-full bg-gradient-radial from-neon-purple/20 via-transparent to-transparent"></div>

          <div className="container relative z-10">
            <motion.div
              className="max-w-4xl mx-auto p-10 bg-dark-100/80 backdrop-blur-md rounded-xl border neon-border"
              initial={{ opacity: 0, y: 30 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ duration: 0.5 }}
            >
              <div className="flex flex-col items-center text-center">
                <motion.h2
                  className="text-3xl md:text-5xl font-display font-bold neon-gradient-text"
                  initial={{ opacity: 0, y: 20 }}
                  whileInView={{ opacity: 1, y: 0 }}
                  viewport={{ once: true }}
                  transition={{ duration: 0.5, delay: 0.1 }}
                >
                  Ready to Transform Your SQL Workflow?
                </motion.h2>

                <motion.p
                  className="mt-4 max-w-[85%] text-lg text-muted-foreground"
                  initial={{ opacity: 0, y: 20 }}
                  whileInView={{ opacity: 1, y: 0 }}
                  viewport={{ once: true }}
                  transition={{ duration: 0.5, delay: 0.2 }}
                >
                  Join thousands of data professionals who are saving time and
                  building better databases with SQLPilot.
                </motion.p>

                <motion.div
                  className="mt-8 flex flex-col sm:flex-row items-center gap-4"
                  initial={{ opacity: 0, y: 20 }}
                  whileInView={{ opacity: 1, y: 0 }}
                  viewport={{ once: true }}
                  transition={{ duration: 0.5, delay: 0.3 }}
                >
                  <Link href="/register">
                    <Button
                      size="lg"
                      className="gap-2 neon-gradient-bg hover:opacity-90 transition-opacity shadow-lg"
                    >
                      Get Started for Free <ArrowRight className="h-4 w-4" />
                    </Button>
                  </Link>
                  <Link href="/login">
                    <Button size="lg" variant="outline" className="neon-border">
                      Schedule a Demo
                    </Button>
                  </Link>
                </motion.div>

                <motion.div
                  className="mt-8 flex items-center gap-2 text-muted-foreground"
                  initial={{ opacity: 0 }}
                  whileInView={{ opacity: 1 }}
                  viewport={{ once: true }}
                  transition={{ duration: 0.5, delay: 0.4 }}
                >
                  <Check className="h-5 w-5 text-neon-teal" />
                  <span>No credit card required</span>
                </motion.div>
              </div>
            </motion.div>
          </div>
        </section>
      </main>

      <footer className="border-t border-white/10 py-12 relative">
        <div className="absolute inset-0 bg-dark-300 noise-bg"></div>
        <div className="container relative z-10 flex flex-col md:flex-row justify-between items-center">
          <div className="flex items-center gap-2 mb-4 md:mb-0">
            <div className="h-8 w-8 rounded-lg neon-gradient-bg flex items-center justify-center text-white">
              <Database className="h-5 w-5" />
            </div>
            <span className="text-xl font-display font-bold neon-gradient-text">
              SQLPilot
            </span>
          </div>
          <div className="flex flex-col md:flex-row gap-4 md:gap-8 items-center">
            <Link
              href="#"
              className="text-sm text-muted-foreground hover:text-foreground transition-colors"
            >
              Privacy Policy
            </Link>
            <Link
              href="#"
              className="text-sm text-muted-foreground hover:text-foreground transition-colors"
            >
              Terms of Service
            </Link>
            <Link
              href="#"
              className="text-sm text-muted-foreground hover:text-foreground transition-colors"
            >
              Contact Us
            </Link>
          </div>
          <div className="mt-4 md:mt-0 text-sm text-muted-foreground">
            © 2025 SQLPilot. All rights reserved.
          </div>
        </div>
      </footer>
    </div>
  );
}
