import { motion } from 'framer-motion';
import React from 'react'
import { Card } from '../ui/card';
import { Download, Plus, Sparkles } from 'lucide-react';
import { Button } from '../ui/button';

const Billing = () => {
  return (
    <motion.div
      key="billing"
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -20 }}
      transition={{ duration: 0.3 }}
    >
      <Card className="bg-background/50 backdrop-blur-md border border-white/10 dark:border-white/10 light:border-gray-200">
        <div className="p-6 border-b border-white/10 dark:border-white/10 light:border-gray-200">
          <div className="flex justify-between items-center">
            <div>
              <h2 className="text-xl font-display font-bold neon-gradient-text">
                Billing & Subscription
              </h2>
              <p className="text-muted-foreground text-sm mt-1">
                Manage your subscription and payment methods
              </p>
            </div>
          </div>
        </div>
        <div className="p-6">
          <div className="space-y-8">
            <div className="bg-background/60 backdrop-blur-sm border border-white/10 rounded-lg p-6 dark:bg-dark-100/60 dark:backdrop-blur-sm dark:border-white/10 light:bg-white light:border-gray-200">
              <div className="flex justify-between items-center">
                <div>
                  <h3 className="text-lg font-medium">Current Plan</h3>
                  <div className="flex items-center mt-2">
                    <div className="h-10 w-10 rounded-full bg-neon-purple/10 flex items-center justify-center mr-3">
                      <Sparkles className="h-5 w-5 text-neon-purple" />
                    </div>
                    <div>
                      <div className="text-xl font-display font-bold neon-gradient-text">
                        Pro Plan
                      </div>
                      <p className="text-sm text-muted-foreground">
                        $49/month • Renews on April 15, 2025
                      </p>
                    </div>
                  </div>
                </div>
                <Button variant="outline" className="neon-border">
                  Change Plan
                </Button>
              </div>

              <div className="mt-6 grid grid-cols-1 md:grid-cols-3 gap-4">
                <div className="bg-background/40 rounded-lg p-4 border border-white/5 dark:bg-dark-200/40 dark:border-white/5 light:bg-gray-50 light:border-gray-100">
                  <h4 className="text-sm font-medium">Queries</h4>
                  <div className="mt-2 text-2xl font-bold">5,000</div>
                  <p className="text-xs text-muted-foreground">per month</p>
                </div>
                <div className="bg-background/40 rounded-lg p-4 border border-white/5 dark:bg-dark-200/40 dark:border-white/5 light:bg-gray-50 light:border-gray-100">
                  <h4 className="text-sm font-medium">Storage</h4>
                  <div className="mt-2 text-2xl font-bold">10 GB</div>
                  <p className="text-xs text-muted-foreground">total</p>
                </div>
                <div className="bg-background/40 rounded-lg p-4 border border-white/5 dark:bg-dark-200/40 dark:border-white/5 light:bg-gray-50 light:border-gray-100">
                  <h4 className="text-sm font-medium">Projects</h4>
                  <div className="mt-2 text-2xl font-bold">20</div>
                  <p className="text-xs text-muted-foreground">maximum</p>
                </div>
              </div>
            </div>

            <div>
              <h3 className="text-lg font-medium mb-4">Payment Methods</h3>
              <div className="space-y-4">
                <div className="bg-background/40 border border-white/10 rounded-lg p-4 dark:bg-dark-200/40 dark:border-white/10 light:bg-gray-50 light:border-gray-200">
                  <div className="flex justify-between items-center">
                    <div className="flex items-center">
                      <div className="h-10 w-10 rounded-lg bg-background/60 flex items-center justify-center mr-3 dark:bg-dark-300/60 light:bg-white">
                        <svg
                          className="h-6 w-6"
                          viewBox="0 0 24 24"
                          fill="none"
                        >
                          <rect
                            x="2"
                            y="5"
                            width="20"
                            height="14"
                            rx="2"
                            fill="#1A1A2E"
                          />
                          <path d="M7 15H4V9H7V15Z" fill="#FF3CA2" />
                          <path d="M11 15H8V9H11V15Z" fill="#9D4EDD" />
                          <path d="M15 15H12V9H15V15Z" fill="#00B4D8" />
                          <path d="M19 15H16V9H19V15Z" fill="#00F5D4" />
                        </svg>
                      </div>
                      <div>
                        <h4 className="text-sm font-medium">
                          Visa ending in 4242
                        </h4>
                        <p className="text-xs text-muted-foreground mt-1">
                          Expires 12/2025
                        </p>
                      </div>
                    </div>
                    <div className="flex items-center">
                      <span className="text-xs px-2 py-0.5 rounded-full bg-green-500/20 text-green-500 mr-2">
                        Default
                      </span>
                      <Button variant="ghost" size="sm" className="text-xs">
                        Edit
                      </Button>
                    </div>
                  </div>
                </div>
                <Button variant="outline" className="neon-border">
                  <Plus className="h-4 w-4 mr-2" />
                  Add Payment Method
                </Button>
              </div>
            </div>

            <div>
              <h3 className="text-lg font-medium mb-4">Billing History</h3>
              <div className="border border-white/10 dark:border-white/10 light:border-gray-200 rounded-md overflow-hidden">
                <table className="w-full">
                  <thead>
                    <tr className="bg-background/60 dark:bg-dark-200/60 light:bg-gray-100">
                      <th className="text-left p-3 border-b border-white/10 dark:border-white/10 light:border-gray-200">
                        Date
                      </th>
                      <th className="text-left p-3 border-b border-white/10 dark:border-white/10 light:border-gray-200">
                        Description
                      </th>
                      <th className="text-left p-3 border-b border-white/10 dark:border-white/10 light:border-gray-200">
                        Amount
                      </th>
                      <th className="text-left p-3 border-b border-white/10 dark:border-white/10 light:border-gray-200">
                        Status
                      </th>
                      <th className="text-center p-3 border-b border-white/10 dark:border-white/10 light:border-gray-200">
                        Invoice
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {[
                      {
                        date: "Mar 15, 2025",
                        description: "Pro Plan - Monthly",
                        amount: "$49.00",
                        status: "Paid",
                      },
                      {
                        date: "Feb 15, 2025",
                        description: "Pro Plan - Monthly",
                        amount: "$49.00",
                        status: "Paid",
                      },
                      {
                        date: "Jan 15, 2025",
                        description: "Pro Plan - Monthly",
                        amount: "$49.00",
                        status: "Paid",
                      },
                    ].map((invoice, index) => (
                      <tr
                        key={index}
                        className="border-b last:border-b-0 border-white/10 dark:border-white/10 light:border-gray-200 hover:bg-background/30 dark:hover:bg-dark-200/30 light:hover:bg-gray-50"
                      >
                        <td className="p-3">{invoice.date}</td>
                        <td className="p-3">{invoice.description}</td>
                        <td className="p-3">{invoice.amount}</td>
                        <td className="p-3">
                          <span className="inline-flex items-center rounded-full px-2 py-1 text-xs font-medium bg-green-500/20 text-green-500">
                            {invoice.status}
                          </span>
                        </td>
                        <td className="p-3 text-center">
                          <Button variant="ghost" size="sm" className="text-xs">
                            <Download className="h-3 w-3 mr-1" />
                            PDF
                          </Button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </div>
      </Card>
    </motion.div>
  );
}

export default Billing