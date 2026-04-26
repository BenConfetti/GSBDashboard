# Score Proxy

Fantrax `Score` lijkt geen simpele optelsom van de zichtbare kolommen te zijn. Daarom gebruiken we een proxy-model dat per seizoen een lineaire benadering fit op basis van de beschikbare seizoenstats.

## Script

Gebruik:

```powershell
python analysis\fit_score_proxy.py --input-dir downloaddata --output-json exports\score_proxy_models.json
```

Het script:

- leest alle `players*.csv` bestanden
- behandelt de niet-vaste kolommen als kandidaat-statfeatures
- fit per seizoen een lineair model
- schrijft de formule en kwaliteitscijfers weg

## Output

Per seizoen krijg je:

- `r_squared`
- `mae`
- `intercept`
- coëfficiënten per stat
- een formule-string

## Interpretatie

Een formule ziet er ongeveer zo uit:

```text
Score ≈ intercept + a*G + b*KP + c*SOT + ...
```

Hoe hoger `R²`, hoe beter de proxy het Fantrax `Score` benadert.
