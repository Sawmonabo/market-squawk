import type * as React from "react"
import { Plus, Trash2 } from "lucide-react"

import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { humanize } from "@/lib/formatters"

import type { DecisionDossierView } from "./contracts"
import {
  TARGET_PRICE_FIELDS,
  type TargetAssumptionDraft,
  type TargetPriceDraft,
  type TargetPriceKey,
} from "./target-builder-model"
import type { TargetPreparationView } from "./target-preparation-contracts"

const SELECT_CLASS =
  "mt-2 h-9 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/50"

export function TargetPriceLadder({
  prices,
  currency,
  onChange,
}: {
  prices: TargetPriceDraft
  currency: string
  onChange: (key: TargetPriceKey, value: string) => void
}) {
  return (
    <fieldset>
      <legend className="text-sm font-semibold">Complete ordered price ladder</legend>
      <p className="mt-1 text-xs leading-5 text-muted-foreground">
        Enter exact {currency} decimals from the lowest downside case through the highest upside
        case. The ranges must stay in this order.
      </p>
      <div className="mt-3 grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
        {TARGET_PRICE_FIELDS.map((field) => (
          <Field key={field.key} label={field.label} htmlFor={`target-price-${field.key}`} help={field.help}>
            <div className="mt-2 flex items-center gap-2">
              <Input
                id={`target-price-${field.key}`}
                value={prices[field.key]}
                onChange={(event) => onChange(field.key, event.target.value)}
                inputMode="decimal"
                autoComplete="off"
                placeholder="0.00"
              />
              <span className="text-[10px] text-muted-foreground">{currency}</span>
            </div>
          </Field>
        ))}
      </div>
    </fieldset>
  )
}

export function AssumptionsEditor({
  assumptions,
  dossier,
  preparation,
  forecastSelected,
  fairValueSelected,
  portfolioSelected,
  onChange,
}: {
  assumptions: TargetAssumptionDraft[]
  dossier: DecisionDossierView
  preparation: TargetPreparationView
  forecastSelected: boolean
  fairValueSelected: boolean
  portfolioSelected: boolean
  onChange: (assumptions: TargetAssumptionDraft[]) => void
}) {
  const evidenceChoices = [
    { value: "dossier", label: "Complete retained dossier" },
    ...dossier.references.map((reference, index) => ({
      value: `dossier_reference:${index}`,
      label: `Dossier reference ${index + 1}: ${humanize(reference.section)}`,
    })),
    ...(forecastSelected ? [{ value: "forecast", label: "Selected forecast evidence" }] : []),
    ...(preparation.fairValueAvailable && fairValueSelected
      ? [{ value: "fair_value", label: "Selected fair-value evidence" }]
      : []),
    ...(preparation.portfolioAvailable && portfolioSelected
      ? [{ value: "portfolio", label: "Selected portfolio evidence" }]
      : []),
    { value: "reference_mark", label: "Selected observed reference mark" },
  ]
  return (
    <fieldset>
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <legend className="text-sm font-semibold">Evidence-bound assumptions</legend>
          <p className="mt-1 text-xs leading-5 text-muted-foreground">
            State each assumption and select the exact retained evidence that supports it.
          </p>
        </div>
        <Button
          type="button"
          variant="outline"
          size="sm"
          disabled={assumptions.length >= 32}
          onClick={() => onChange([...assumptions, { text: "", evidenceKey: "dossier" }])}
        >
          <Plus aria-hidden="true" />
          Add assumption
        </Button>
      </div>
      <div className="mt-3 grid gap-3">
        {assumptions.map((assumption, index) => (
          <div
            key={index}
            className="grid gap-3 rounded-xl border border-border bg-background/45 p-4 lg:grid-cols-[minmax(0,1fr)_minmax(260px,0.6fr)_auto]"
          >
            <Field label={`Assumption ${index + 1}`} htmlFor={`target-assumption-${index}`}>
              <Input
                id={`target-assumption-${index}`}
                className="mt-2"
                maxLength={4_096}
                value={assumption.text}
                onChange={(event) =>
                  onChange(
                    assumptions.map((item, itemIndex) =>
                      itemIndex === index ? { ...item, text: event.target.value } : item,
                    ),
                  )
                }
              />
            </Field>
            <Field label="Supporting evidence" htmlFor={`target-assumption-evidence-${index}`}>
              <select
                id={`target-assumption-evidence-${index}`}
                className={SELECT_CLASS}
                value={assumption.evidenceKey}
                onChange={(event) =>
                  onChange(
                    assumptions.map((item, itemIndex) =>
                      itemIndex === index
                        ? { ...item, evidenceKey: event.target.value }
                        : item,
                    ),
                  )
                }
              >
                {evidenceChoices.map((choice) => (
                  <option key={choice.value} value={choice.value}>{choice.label}</option>
                ))}
              </select>
            </Field>
            <Button
              type="button"
              variant="ghost"
              size="icon"
              className="self-end"
              disabled={assumptions.length === 1}
              aria-label={`Remove assumption ${index + 1}`}
              onClick={() => onChange(assumptions.filter((_item, itemIndex) => itemIndex !== index))}
            >
              <Trash2 aria-hidden="true" />
            </Button>
          </div>
        ))}
      </div>
    </fieldset>
  )
}

export function NarrativeList({
  title,
  description,
  singular,
  values,
  onChange,
}: {
  title: string
  description: string
  singular: string
  values: string[]
  onChange: (values: string[]) => void
}) {
  return (
    <fieldset>
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <legend className="text-sm font-semibold">{title}</legend>
          <p className="mt-1 text-xs leading-5 text-muted-foreground">{description}</p>
        </div>
        <Button
          type="button"
          variant="outline"
          size="sm"
          disabled={values.length >= 32}
          onClick={() => onChange([...values, ""])}
        >
          <Plus aria-hidden="true" />
          Add {singular.toLowerCase()}
        </Button>
      </div>
      <div className="mt-3 grid gap-2">
        {values.map((value, index) => (
          <div key={index} className="flex items-start gap-2">
            <Input
              aria-label={`${singular} ${index + 1}`}
              maxLength={4_096}
              value={value}
              onChange={(event) =>
                onChange(values.map((item, itemIndex) => itemIndex === index ? event.target.value : item))
              }
            />
            <Button
              type="button"
              variant="ghost"
              size="icon"
              disabled={values.length === 1}
              aria-label={`Remove ${singular.toLowerCase()} ${index + 1}`}
              onClick={() => onChange(values.filter((_item, itemIndex) => itemIndex !== index))}
            >
              <Trash2 aria-hidden="true" />
            </Button>
          </div>
        ))}
      </div>
    </fieldset>
  )
}

export function ThesisField({ value, onChange }: { value: string; onChange: (value: string) => void }) {
  return (
    <Field
      label="Investment thesis"
      htmlFor="target-thesis"
      help="Explain why the evidence supports this target judgment and what must remain true."
    >
      <Input
        id="target-thesis"
        className="mt-2"
        maxLength={4_096}
        value={value}
        onChange={(event) => onChange(event.target.value)}
      />
    </Field>
  )
}

export function Field({
  label,
  htmlFor,
  help,
  children,
}: {
  label: string
  htmlFor: string
  help?: string
  children: React.ReactNode
}) {
  return (
    <div>
      <Label htmlFor={htmlFor}>{label}</Label>
      {children}
      {help && <p className="mt-1.5 text-[11px] leading-4 text-muted-foreground">{help}</p>}
    </div>
  )
}

export { SELECT_CLASS }
