import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'
import { corsHeaders } from '../_shared/cors.ts'

Deno.serve(async (req) => {
  if (req.method === 'OPTIONS') {
    return new Response('ok', { headers: corsHeaders })
  }

  try {
    // Leer el nombre de la tabla y la columna de conflicto desde la URL
    const url = new URL(req.url)
    const tableName = url.searchParams.get('table')
    const conflictColumn = url.searchParams.get('onConflict')

    if (!tableName || !conflictColumn) {
      throw new Error("Faltan los parámetros 'table' o 'onConflict' en la URL.")
    }

    const records = await req.json()
    if (!Array.isArray(records) || records.length === 0) {
      throw new Error('No se recibieron datos válidos.')
    }

    const supabaseClient = createClient(
      Deno.env.get('SUPABASE_URL') ?? '',
      Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? ''
    )

    const { data, error } = await supabaseClient
      .from(tableName) // Usa el nombre de la tabla dinámicamente
      .upsert(records, { onConflict: conflictColumn }) // Usa la columna de conflicto dinámicamente

    if (error) { throw error }

    return new Response(JSON.stringify({ message: `${(data || []).length} registros procesados en la tabla '${tableName}'.` }), {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      status: 200,
    })
  } catch (err) {
    return new Response(String(err?.message ?? err), {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      status: 500,
    })
  }
})