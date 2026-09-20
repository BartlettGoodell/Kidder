const crypto = require('crypto');

const PROPERTY_ID = process.env.GA_PROPERTY_ID || '555156723';
const ANALYTICS_SCOPE = 'https://www.googleapis.com/auth/analytics.readonly';

function encode(value) {
  return Buffer.from(value).toString('base64url');
}

async function getAccessToken(clientEmail, privateKey) {
  const now = Math.floor(Date.now() / 1000);
  const header = encode(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const payload = encode(JSON.stringify({
    iss: clientEmail,
    scope: ANALYTICS_SCOPE,
    aud: 'https://oauth2.googleapis.com/token',
    iat: now,
    exp: now + 3600
  }));
  const unsignedToken = `${header}.${payload}`;
  const signature = crypto
    .sign('RSA-SHA256', Buffer.from(unsignedToken), privateKey)
    .toString('base64url');

  const response = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
      assertion: `${unsignedToken}.${signature}`
    })
  });

  if (!response.ok) {
    throw new Error(`OAuth token request failed (${response.status})`);
  }

  return (await response.json()).access_token;
}

async function getArticleCounts(accessToken) {
  const response = await fetch(
    `https://analyticsdata.googleapis.com/v1beta/properties/${PROPERTY_ID}:runReport`,
    {
      method: 'POST',
      headers: {
        authorization: `Bearer ${accessToken}`,
        'content-type': 'application/json'
      },
      body: JSON.stringify({
        dateRanges: [{ startDate: '2026-09-20', endDate: 'today' }],
        dimensions: [{ name: 'customEvent:article_title' }],
        metrics: [{ name: 'eventCount' }],
        dimensionFilter: {
          filter: {
            fieldName: 'eventName',
            stringFilter: { matchType: 'EXACT', value: 'select_content' }
          }
        },
        orderBys: [{ metric: { metricName: 'eventCount' }, desc: true }],
        limit: 500
      })
    }
  );

  if (!response.ok) {
    throw new Error(`Analytics report failed (${response.status})`);
  }

  const report = await response.json();
  return Object.fromEntries((report.rows || []).map(row => [
    row.dimensionValues?.[0]?.value || '',
    Number(row.metricValues?.[0]?.value || 0)
  ]).filter(([title]) => title));
}

module.exports = async function handler(req, res) {
  if (req.method !== 'GET') {
    res.setHeader('Allow', 'GET');
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const clientEmail = process.env.GA_CLIENT_EMAIL;
  const privateKey = process.env.GA_PRIVATE_KEY?.replace(/\\n/g, '\n');
  if (!clientEmail || !privateKey) {
    return res.status(503).json({ error: 'Analytics reporting is not configured' });
  }

  try {
    const accessToken = await getAccessToken(clientEmail, privateKey);
    const counts = await getArticleCounts(accessToken);
    res.setHeader('Cache-Control', 'public, s-maxage=3600, stale-while-revalidate=86400');
    return res.status(200).json({ counts, updatedAt: new Date().toISOString() });
  } catch (error) {
    console.error('Unable to load article counts:', error.message);
    return res.status(502).json({ error: 'Article counts are temporarily unavailable' });
  }
};
